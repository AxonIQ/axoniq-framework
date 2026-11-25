/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.springboot.workflow;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.configuration.WorkflowConfigurationDefaults;
import io.axoniq.framework.workflow.configuration.WorkflowSnapshotConfiguration;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedRunningWorkflows;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.modelling.repository.ManagedEntity;
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Verifies, on the real autoconfiguration chain, that a {@link SnapshotStore} bean turns on snapshots of the workflow
 * engine's entities unless {@code axon.workflow.snapshots.enabled} is {@code false}, and that an application without a
 * {@code SnapshotStore} starts and runs its workflows without snapshots unless snapshots are enabled.
 * {@code SnapshotPolicy} beans under the deprecated names of {@link WorkflowConfigurationDefaults} snapshot their
 * entity only while that property is not set.
 *
 * @author Allard Buijze
 */
class WorkflowSnapshotsSpringContextTest {

    private static final String START_EVENT = "io.axoniq.snapshots.SnapshotTestStarted";
    // Every step records a started and a completed event, so the state outgrows the default policy of 50 events.
    private static final int STEPS = 30;
    // Snapshots the running-workflow list after any sourced event, whenever its snapshots are on.
    private static final String EVERY_RUNNING_WORKFLOWS_EVENT =
            "--axon.workflow.snapshots.running-workflows.after-events=0";

    @Nested
    class WithASnapshotStoreBean {

        @Test
        void snapshotsOnlyWorkflowStateByDefault() {
            try (var context = contextWith(WithSnapshotStore.class, EVERY_RUNNING_WORKFLOWS_EVENT)) {
                // given
                var configuration = context.getBean(AxonConfiguration.class);

                // when
                var workflowId = runWorkflowToCompletion(configuration);
                loadRunningWorkflows(configuration);

                // then
                await().atMost(10, TimeUnit.SECONDS)
                       .until(() -> storedSnapshot(configuration, workflowId) != null);
                assertThat(storedRunningWorkflowsSnapshot(configuration)).isNull();
            }
        }

        @ParameterizedTest
        @ValueSource(strings = {"true", "yes", "on"})
        void enabledSnapshotsSnapshotTheEntities(String enabled) {
            try (var context = contextWith(WithSnapshotStore.class,
                                           "--axon.workflow.snapshots.enabled=" + enabled,
                                           EVERY_RUNNING_WORKFLOWS_EVENT)) {
                // given
                var configuration = context.getBean(AxonConfiguration.class);

                // when
                var workflowId = runWorkflowToCompletion(configuration);
                loadRunningWorkflows(configuration);

                // then
                await().atMost(10, TimeUnit.SECONDS)
                       .until(() -> storedSnapshot(configuration, workflowId) != null);
                assertThat(storedRunningWorkflowsSnapshot(configuration)).isNotNull();
            }
        }

        @Test
        void disabledSnapshotsTakeNoSnapshot() {
            try (var context = contextWith(WithSnapshotStore.class,
                                           "--axon.workflow.snapshots.enabled=false",
                                           EVERY_RUNNING_WORKFLOWS_EVENT)) {
                // given
                var configuration = context.getBean(AxonConfiguration.class);

                // when
                var workflowId = runWorkflowToCompletion(configuration);
                loadState(configuration, workflowId);
                loadRunningWorkflows(configuration);

                // then
                assertThat(storedSnapshot(configuration, workflowId)).isNull();
                assertThat(storedRunningWorkflowsSnapshot(configuration)).isNull();
            }
        }

        @Test
        void snapshotConfigurationBeanReplacesTheProperties() {
            try (var context = contextWith(WithDisabledSnapshotConfiguration.class,
                                           "--axon.workflow.snapshots.enabled=true",
                                           EVERY_RUNNING_WORKFLOWS_EVENT)) {
                // given
                var configuration = context.getBean(AxonConfiguration.class);

                // when
                var workflowId = runWorkflowToCompletion(configuration);
                loadState(configuration, workflowId);
                loadRunningWorkflows(configuration);

                // then - the bean disables snapshots, whatever the properties say
                assertThat(storedSnapshot(configuration, workflowId)).isNull();
                assertThat(storedRunningWorkflowsSnapshot(configuration)).isNull();
            }
        }
    }

    @Nested
    class WithSnapshotPolicyBeansUnderTheirDeprecatedNames {

        // The policies of the properties would not snapshot the few events of these entities.
        private static final String[] RARE_PROPERTY_POLICIES = {
                "--axon.workflow.snapshots.running-workflows.after-events=1000",
                "--axon.workflow.snapshots.workflow-state.after-events=1000"
        };

        @Test
        void namedPolicyBeansSnapshotBothEntitiesWhenSnapshotsAreNotSet() {
            try (var context = contextWith(WithNamedSnapshotPolicies.class, RARE_PROPERTY_POLICIES)) {
                // given
                var configuration = context.getBean(AxonConfiguration.class);

                // when
                var workflowId = runWorkflowToCompletion(configuration);
                loadState(configuration, workflowId);
                loadRunningWorkflows(configuration);

                // then - the named beans take precedence over the policies of the properties
                await().atMost(10, TimeUnit.SECONDS)
                       .until(() -> storedSnapshot(configuration, workflowId) != null);
                await().atMost(10, TimeUnit.SECONDS)
                       .until(() -> storedRunningWorkflowsSnapshot(configuration) != null);
            }
        }

        @ParameterizedTest
        @ValueSource(strings = {"true", "false"})
        void namedPolicyBeansAreIgnoredWhenSnapshotsAreSet(String enabled) {
            try (var context = contextWith(WithNamedSnapshotPolicies.class,
                                           "--axon.workflow.snapshots.enabled=" + enabled,
                                           RARE_PROPERTY_POLICIES[0],
                                           RARE_PROPERTY_POLICIES[1])) {
                // given
                var configuration = context.getBean(AxonConfiguration.class);

                // when
                var workflowId = runWorkflowToCompletion(configuration);
                loadState(configuration, workflowId);
                loadRunningWorkflows(configuration);

                // then - true uses the policies of the properties, false takes no snapshot
                assertThat(storedSnapshot(configuration, workflowId)).isNull();
                assertThat(storedRunningWorkflowsSnapshot(configuration)).isNull();
            }
        }
    }

    @Nested
    class WithoutASnapshotStoreBean {

        @Test
        void runsWorkflowsWithoutSnapshots() {
            try (var context = contextWith(WithoutSnapshotStore.class)) {
                // given
                var configuration = context.getBean(AxonConfiguration.class);
                assertThat(configuration.hasComponent(SnapshotStore.class)).isFalse();

                // when
                var workflowId = runWorkflowToCompletion(configuration);

                // then
                assertThat(loadState(configuration, workflowId).workflowStepNames()).hasSize(STEPS);
            }
        }

        @Test
        void enabledSnapshotsFailTheStart() {
            // when / then
            assertThatThrownBy(() -> contextWith(WithoutSnapshotStore.class, "--axon.workflow.snapshots.enabled=true"))
                    .hasStackTraceContaining("A SnapshotStore must be configured to use snapshotting.");
        }
    }

    private static ConfigurableApplicationContext contextWith(Class<?> application, String... properties) {
        var arguments = new String[properties.length + 2];
        arguments[0] = "--axon.axonserver.enabled=false";
        arguments[1] = "--axon.postgresql.enabled=false";
        System.arraycopy(properties, 0, arguments, 2, properties.length);
        return new SpringApplicationBuilder(application).web(WebApplicationType.NONE).run(arguments);
    }

    private static String runWorkflowToCompletion(AxonConfiguration configuration) {
        var workflowId = "order-1";
        var event = new GenericEventMessage(new MessageType(START_EVENT), Map.of("orderId", workflowId));
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-start")
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
        await().atMost(30, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> loadState(configuration, workflowId).workflowStatus() == WorkflowStatus.COMPLETED);
        return workflowId;
    }

    @SuppressWarnings("unchecked")
    private static EventSourcedWorkflowState loadState(AxonConfiguration configuration, String workflowId) {
        var repository = (Repository<String, EventSourcedWorkflowState>) configuration
                .getComponents(Repository.class)
                .values()
                .stream()
                .filter(candidate -> candidate.entityType().equals(EventSourcedWorkflowState.class))
                .findFirst()
                .orElseThrow();
        return configuration.getComponent(UnitOfWorkFactory.class)
                            .create("load-state")
                            .executeWithResult(ctx -> repository.loadOrCreate(workflowId, ctx)
                                                                .thenApply(ManagedEntity::entity))
                            .orTimeout(5, TimeUnit.SECONDS)
                            .join();
    }

    @SuppressWarnings("unchecked")
    private static void loadRunningWorkflows(AxonConfiguration configuration) {
        var repository = (Repository<String, EventSourcedRunningWorkflows>) configuration
                .getComponents(Repository.class)
                .values()
                .stream()
                .filter(candidate -> candidate.entityType().equals(EventSourcedRunningWorkflows.class))
                .findFirst()
                .orElseThrow();
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("load-running-workflows")
                     .executeWithResult(ctx -> repository.loadOrCreate(EventSourcedRunningWorkflows.ENTITY_ID, ctx))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
    }

    @Nullable
    private static Snapshot storedRunningWorkflowsSnapshot(AxonConfiguration configuration) {
        var snapshotName = configuration.getComponent(MessageTypeResolver.class)
                                        .resolveOrThrow(EventSourcedRunningWorkflows.class)
                                        .qualifiedName();
        var snapshotStore = configuration.getComponent(SnapshotStore.class);
        var snapshot = snapshotStore.load(snapshotName, EventSourcedRunningWorkflows.ENTITY_ID, null);
        return FutureUtils.joinAndUnwrap(snapshot);
    }

    @Nullable
    private static Snapshot storedSnapshot(AxonConfiguration configuration, String workflowId) {
        var snapshotName = configuration.getComponent(MessageTypeResolver.class)
                                        .resolveOrThrow(EventSourcedWorkflowState.class)
                                        .qualifiedName();
        return FutureUtils.joinAndUnwrap(
                configuration.getComponent(SnapshotStore.class).load(snapshotName, workflowId, null)
        );
    }

    @Configuration
    @EnableAutoConfiguration
    static class WithoutSnapshotStore {

        @Bean
        EventStorageEngine eventStorageEngine() {
            return new InMemoryEventStorageEngine();
        }

        @Bean
        TokenStore tokenStore() {
            return new InMemoryTokenStore();
        }

        @Bean
        ManyStepsWorkflow manyStepsWorkflow() {
            return new ManyStepsWorkflow();
        }
    }

    @Configuration
    static class WithSnapshotStore extends WithoutSnapshotStore {

        @Bean
        SnapshotStore snapshotStore() {
            return new InMemorySnapshotStore(new JacksonConverter());
        }
    }

    @Configuration
    static class WithDisabledSnapshotConfiguration extends WithSnapshotStore {

        @Bean
        WorkflowSnapshotConfiguration workflowSnapshotConfiguration() {
            return new WorkflowSnapshotConfiguration().enabled(false);
        }
    }

    @Configuration
    @SuppressWarnings("deprecation")
    static class WithNamedSnapshotPolicies extends WithSnapshotStore {

        @Bean(WorkflowConfigurationDefaults.WORKFLOW_STATE_SNAPSHOT_POLICY)
        SnapshotPolicy workflowStateSnapshotPolicy() {
            return SnapshotPolicy.afterEvents(1);
        }

        @Bean(WorkflowConfigurationDefaults.RUNNING_WORKFLOWS_SNAPSHOT_POLICY)
        SnapshotPolicy runningWorkflowsSnapshotPolicy() {
            return SnapshotPolicy.afterEvents(0);
        }
    }

    public static class ManyStepsWorkflow {

        @Workflow(workflowName = "ManyStepsWorkflow", idProperty = "orderId", startOnEventName = START_EVENT)
        public void run(SimpleWorkflowContext context) {
            for (int step = 0; step < STEPS; step++) {
                context.awaitExecute("step-" + step, Boolean.class, () -> true);
            }
        }
    }
}
