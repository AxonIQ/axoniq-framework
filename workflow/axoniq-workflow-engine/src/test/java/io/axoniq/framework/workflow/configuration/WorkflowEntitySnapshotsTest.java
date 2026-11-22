/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedRunningWorkflows;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.PassThroughConverter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.NamespaceMessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.annotation.AnnotationMessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.dsl.api.StepStatus.COMPLETED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies when the default {@link WorkflowConfigurer} wiring snapshots the entities of the workflow engine, as the
 * {@link WorkflowSnapshotConfiguration} decides: by default the state of each workflow instance whenever the
 * application can take snapshots, nothing when snapshots are disabled, and both the state and the running-workflow
 * list when they are enabled.
 *
 * @author Allard Buijze
 */
class WorkflowEntitySnapshotsTest extends AbstractEventSourcedEntityRepositoryTestBase {

    private static final String MODULE = "workflow-entity-snapshots";
    private static final VersionedType DEFINITION_ID = VersionedType.of(new QualifiedName("OrderWorkflow"), "1.0.0");

    private final Converter converter = new JacksonConverter();
    private final SnapshotStore snapshotStore = new InMemorySnapshotStore(converter);

    @Nested
    class WhenSnapshotsAreNotConfigured {

        @Test
        void snapshotsWorkflowStateAfterFiftyEventsWhenTheApplicationHasASnapshotStore() {
            // given
            configuration = configuration(MODULE, configurer -> configurer.componentRegistry(cr -> registerStore(cr)));
            configuration.start();
            publishWorkflowWithEvents("wf-short", 10);
            publishWorkflowWithEvents("wf-long", 60);

            // when
            load("wf-short");
            load("wf-long");
            var restored = load("wf-long");

            // then - only the state sourced from more events than the default policy allows has a snapshot
            assertThat(storedSnapshot("wf-short")).isNull();
            assertThat(storedSnapshot("wf-long")).isNotNull();
            assertThat(restored.workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
            assertThat(restored.workflowDefinitionId()).isEqualTo(DEFINITION_ID);
            assertThat(restored.workflowStepNames()).hasSize(59);
            assertThat(restored.getStep("step-58").status()).isEqualTo(COMPLETED);
            assertThat(restored.payload()).containsEntry("step-58", "done");
        }

        @Test
        void stateContinuedFromTheDefaultSnapshotEqualsTheStateReplayedFromEveryEvent() {
            // given - a failed workflow with many steps, a failed step and a recorded version migration
            configuration = configuration(MODULE, configurer -> configurer.componentRegistry(cr -> registerStore(cr)));
            configuration.start();
            var customizer = DefaultEventNameCustomizer.Builder.defaults();
            var context = workflowExecutionOperations("wf-1", DEFINITION_ID.version());
            publishWorkflowWithEvents("wf-1", 55);
            publish(EventMessageUtils.versionMigrationStep(context, "payment-redesign", "1.1.0", customizer));
            publish(EventMessageUtils.failStep(
                    context, "chargeCard", new IllegalStateException("card declined"), customizer
            ));
            publish(EventMessageUtils.failedWorkflow(
                    context, "OrderWorkflow", new IllegalStateException("payment failed"), DEFINITION_ID, customizer
            ));

            // when - the first load replays every event and stores a snapshot, the second continues from it
            var replayed = load("wf-1");
            assertThat(storedSnapshot("wf-1")).isNotNull();
            var restored = load("wf-1");

            // then
            assertThat(restored).isNotSameAs(replayed);
            assertThat(restored.workflowId()).isEqualTo(replayed.workflowId());
            assertThat(restored.workflowStatus()).isEqualTo(WorkflowStatus.FAILED);
            assertThat(restored.workflowDefinitionId()).isEqualTo(replayed.workflowDefinitionId());
            assertThat(restored.payload()).isEqualTo(replayed.payload()).containsEntry("step-53", "done");
            assertThat(restored.versionMigrations()).isEqualTo(replayed.versionMigrations())
                                                    .containsEntry("payment-redesign", "1.1.0");
            assertThat(restored.workflowStepNames()).containsExactlyInAnyOrderElementsOf(replayed.workflowStepNames());
            assertThat(restored.workflowStepNames()).allSatisfy(stepName -> {
                assertThat(restored.getStep(stepName).status()).isEqualTo(replayed.getStep(stepName).status());
                assertThat(restored.getStep(stepName).timestamp()).isEqualTo(replayed.getStep(stepName).timestamp());
            });
            assertThat(restored.getStep("chargeCard").error()).hasMessage("card declined");
            assertThatThrownBy(restored::throwTerminalCause).hasMessageContaining("payment failed");
            assertThatThrownBy(replayed::throwTerminalCause).hasMessageContaining("payment failed");
        }

        @Test
        void snapshotsWorkflowStateWithTheConfiguredPolicyWithoutEnablingSnapshots() {
            // given - a workflow-state policy, while snapshots are neither enabled nor disabled
            configuration = configuration(MODULE, configurer -> configurer
                    .componentRegistry(cr -> registerStore(cr))
                    .snapshots(snapshots -> snapshots.workflowState(SnapshotPolicy.afterEvents(1))));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 3);

            // when
            load("wf-1");

            // then
            assertThat(storedSnapshot("wf-1")).isNotNull();
        }

        @Test
        void doesNotSnapshotTheRunningWorkflowList() {
            // given - a running-workflow policy, while snapshots are neither enabled nor disabled
            configuration = configuration(MODULE, configurer -> configurer
                    .componentRegistry(cr -> registerStore(cr))
                    .snapshots(snapshots -> snapshots.runningWorkflows(SnapshotPolicy.afterEvents(1))));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 3);
            publishWorkflowWithEvents("wf-2", 3);

            // when
            var runningWorkflows = loadRunningWorkflows();

            // then
            assertThat(runningWorkflows.workflowIds()).containsExactlyInAnyOrder("wf-1", "wf-2");
            assertThat(storedRunningWorkflowsSnapshot()).isNull();
        }

        @Test
        void startsWithoutSnapshotsWhenTheApplicationHasNoSnapshotStore() {
            // given
            configuration = configuration(MODULE);

            // when
            configuration.start();
            publishWorkflowWithEvents("wf-1", 60);
            var state = load("wf-1");

            // then - the state is sourced from every event
            assertThat(configuration.hasComponent(SnapshotStore.class)).isFalse();
            assertThat(state.workflowStepNames()).hasSize(59);
        }

        @Test
        void startsWithoutSnapshotsWhenTheMessageTypeResolverDoesNotResolveTheWorkflowState() {
            // given - a resolver that only knows the types it is told about, and is not told about the workflow state
            configuration = configuration(MODULE, configurer -> configurer.componentRegistry(cr -> {
                registerStore(cr);
                cr.registerComponent(MessageTypeResolver.class,
                                     c -> NamespaceMessageTypeResolver.namespace("orders").noFallback());
            }));

            // when
            configuration.start();
            publishWorkflowWithEvents("wf-1", 60);
            load("wf-1");
            var state = load("wf-1");

            // then - the state is sourced from every event, and no snapshot is stored under the name the default
            // resolver gives the state, which is the only name a snapshotting module could have used
            assertThat(state.workflowStepNames()).hasSize(59);
            assertThat(storedSnapshot("wf-1")).isNull();
        }
    }

    @Nested
    class WhenSnapshotsAreDisabled {

        @Test
        void takesNoSnapshotOfEitherEntityEvenWhenTheApplicationHasASnapshotStore() {
            // given
            configuration = configuration(MODULE, configurer -> configurer
                    .componentRegistry(cr -> registerStore(cr))
                    .snapshots(snapshots -> snapshots.enabled(false)
                                                     .runningWorkflows(SnapshotPolicy.afterEvents(1))
                                                     .workflowState(SnapshotPolicy.afterEvents(1))));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 60);
            publishWorkflowWithEvents("wf-2", 3);

            // when
            load("wf-1");
            var state = load("wf-1");
            loadRunningWorkflows();

            // then
            assertThat(storedSnapshot("wf-1")).isNull();
            assertThat(storedRunningWorkflowsSnapshot()).isNull();
            assertThat(state.workflowStepNames()).hasSize(59);
        }
    }

    @Nested
    @SuppressWarnings("deprecation")
    class WhenSnapshotPoliciesAreRegisteredUnderTheirDeprecatedNames {

        private final Logger moduleLogger = (Logger) LogManager.getLogger(WorkflowEntityModule.class);
        private final ListAppender warnings = new ListAppender("WorkflowEntityModuleWarnings");

        @BeforeEach
        void captureWarnings() {
            warnings.start();
            moduleLogger.addAppender(warnings);
        }

        @AfterEach
        void releaseWarnings() {
            moduleLogger.removeAppender(warnings);
            warnings.stop();
        }

        @Test
        void snapshotsWorkflowStateWithItsNamedPolicyWhenSnapshotsAreNotConfigured() {
            // given - a named policy snapshotting sooner than the default policy would
            configuration = configuration(MODULE, configurer -> configurer.componentRegistry(cr -> {
                registerStore(cr);
                registerNamedPolicy(cr, WorkflowConfigurationDefaults.WORKFLOW_STATE_SNAPSHOT_POLICY, 1);
            }));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 3);
            publishWorkflowWithEvents("wf-2", 3);

            // when
            load("wf-1");
            loadRunningWorkflows();

            // then
            assertThat(storedSnapshot("wf-1")).isNotNull();
            assertThat(storedRunningWorkflowsSnapshot()).isNull();
            assertThat(warningMessages()).anySatisfy(message -> assertThat(message)
                    .contains("[" + WorkflowConfigurationDefaults.WORKFLOW_STATE_SNAPSHOT_POLICY + "] is deprecated")
                    .contains(EventSourcedWorkflowState.class.getName())
                    .contains("takes precedence over the WorkflowSnapshotConfiguration's policy"));
        }

        @Test
        void snapshotsTheRunningWorkflowListWithItsNamedPolicyWhenSnapshotsAreNotConfigured() {
            // given
            configuration = configuration(MODULE, configurer -> configurer.componentRegistry(cr -> {
                registerStore(cr);
                registerNamedPolicy(cr, WorkflowConfigurationDefaults.RUNNING_WORKFLOWS_SNAPSHOT_POLICY, 1);
            }));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 3);
            publishWorkflowWithEvents("wf-2", 3);

            // when
            var runningWorkflows = loadRunningWorkflows();

            // then
            assertThat(runningWorkflows.workflowIds()).containsExactlyInAnyOrder("wf-1", "wf-2");
            assertThat(storedRunningWorkflowsSnapshot()).isNotNull();
            assertThat(warningMessages()).anySatisfy(message -> assertThat(message)
                    .contains("[" + WorkflowConfigurationDefaults.RUNNING_WORKFLOWS_SNAPSHOT_POLICY + "] is deprecated")
                    .contains(EventSourcedRunningWorkflows.class.getName())
                    .contains("takes precedence over the WorkflowSnapshotConfiguration's policy"));
        }

        @Test
        void failsToStartWithANamedPolicyWhenTheApplicationHasNoSnapshotStore() {
            // given - a named policy requires snapshots, as it did before 5.5.0
            configuration = configuration(MODULE, configurer -> configurer.componentRegistry(
                    cr -> registerNamedPolicy(cr, WorkflowConfigurationDefaults.WORKFLOW_STATE_SNAPSHOT_POLICY, 1)
            ));

            // when / then
            assertThatThrownBy(() -> configuration.start())
                    .hasStackTraceContaining("A SnapshotStore must be configured to use snapshotting.");
        }

        @Test
        void ignoresNamedPoliciesWhenSnapshotsAreDisabled() {
            // given
            configuration = configuration(MODULE, configurer -> configurer
                    .componentRegistry(cr -> {
                        registerStore(cr);
                        registerNamedPolicy(cr, WorkflowConfigurationDefaults.RUNNING_WORKFLOWS_SNAPSHOT_POLICY, 1);
                        registerNamedPolicy(cr, WorkflowConfigurationDefaults.WORKFLOW_STATE_SNAPSHOT_POLICY, 1);
                    })
                    .snapshots(snapshots -> snapshots.enabled(false)));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 3);
            publishWorkflowWithEvents("wf-2", 3);

            // when
            load("wf-1");
            loadRunningWorkflows();

            // then
            assertThat(storedSnapshot("wf-1")).isNull();
            assertThat(storedRunningWorkflowsSnapshot()).isNull();
            assertIgnoredWarnings("disables");
        }

        @Test
        void usesThePoliciesOfTheConfigurationWhenSnapshotsAreEnabled() {
            // given - named policies that would not snapshot these few events, and configured policies that do
            configuration = configuration(MODULE, configurer -> configurer
                    .componentRegistry(cr -> {
                        registerStore(cr);
                        registerNamedPolicy(cr, WorkflowConfigurationDefaults.RUNNING_WORKFLOWS_SNAPSHOT_POLICY, 1000);
                        registerNamedPolicy(cr, WorkflowConfigurationDefaults.WORKFLOW_STATE_SNAPSHOT_POLICY, 1000);
                    })
                    .snapshots(snapshots -> snapshots.enabled(true)
                                                     .runningWorkflows(SnapshotPolicy.afterEvents(1))
                                                     .workflowState(SnapshotPolicy.afterEvents(1))));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 3);
            publishWorkflowWithEvents("wf-2", 3);

            // when
            load("wf-1");
            loadRunningWorkflows();

            // then
            assertThat(storedSnapshot("wf-1")).isNotNull();
            assertThat(storedRunningWorkflowsSnapshot()).isNotNull();
            assertIgnoredWarnings("enables");
        }

        private void assertIgnoredWarnings(String setting) {
            assertThat(warningMessages())
                    .anySatisfy(message -> assertThat(message).contains(
                            "[" + WorkflowConfigurationDefaults.WORKFLOW_STATE_SNAPSHOT_POLICY + "] is ignored, as the "
                                    + "WorkflowSnapshotConfiguration " + setting + " snapshots"))
                    .anySatisfy(message -> assertThat(message).contains(
                            "[" + WorkflowConfigurationDefaults.RUNNING_WORKFLOWS_SNAPSHOT_POLICY + "] is ignored, as "
                                    + "the WorkflowSnapshotConfiguration " + setting + " snapshots"))
                    .noneSatisfy(message -> assertThat(message).contains("is deprecated"));
        }

        private List<String> warningMessages() {
            return warnings.getEvents()
                           .stream()
                           .filter(event -> event.getLevel() == Level.WARN)
                           .map(event -> event.getMessage().getFormattedMessage())
                           .toList();
        }

        private void registerNamedPolicy(ComponentRegistry componentRegistry, String name, int events) {
            componentRegistry.registerComponent(SnapshotPolicy.class, name, c -> SnapshotPolicy.afterEvents(events));
        }
    }

    @Nested
    class WhenSnapshotsAreEnabled {

        @Test
        void snapshotsBothEntitiesWithTheirConfiguredPolicies() {
            // given - two workflows, so the running-workflow list is sourced from more events than its policy allows
            configuration = configuration(MODULE, configurer -> configurer
                    .componentRegistry(cr -> registerStore(cr))
                    .snapshots(snapshots -> snapshots.enabled(true)
                                                     .runningWorkflows(SnapshotPolicy.afterEvents(1))
                                                     .workflowState(SnapshotPolicy.afterEvents(1))));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 3);
            publishWorkflowWithEvents("wf-2", 3);

            // when
            load("wf-1");
            loadRunningWorkflows();

            // then
            assertThat(storedSnapshot("wf-1")).isNotNull();
            assertThat(storedRunningWorkflowsSnapshot()).isNotNull();
        }

        @Test
        void failsToStartWhenTheApplicationHasNoSnapshotStore() {
            // given
            configuration = configuration(MODULE, configurer -> configurer
                    .snapshots(snapshots -> snapshots.enabled(true)));

            // when / then
            assertThatThrownBy(() -> configuration.start())
                    .hasStackTraceContaining("A SnapshotStore must be configured to use snapshotting.");
        }

        @Test
        void failsToStartWhenTheMessageTypeResolverDoesNotResolveTheWorkflowState() {
            // given
            configuration = configuration(MODULE, configurer -> configurer
                    .componentRegistry(cr -> {
                        registerStore(cr);
                        cr.registerComponent(MessageTypeResolver.class,
                                             c -> NamespaceMessageTypeResolver.namespace("orders").noFallback());
                    })
                    .snapshots(snapshots -> snapshots.enabled(true)));

            // when / then
            assertThatThrownBy(() -> configuration.start())
                    .hasStackTraceContaining("A MessageTypeResolver capable of resolving");
        }
    }

    @Nested
    class WhenAWorkflowSnapshotConfigurationComponentIsRegistered {

        @Test
        void enabledConfigurationSnapshotsBothEntities() {
            // given
            configuration = configuration(MODULE, configurer -> configurer.componentRegistry(cr -> {
                registerStore(cr);
                cr.registerComponent(WorkflowSnapshotConfiguration.class,
                                     c -> new WorkflowSnapshotConfiguration()
                                             .enabled(true)
                                             .runningWorkflows(SnapshotPolicy.afterEvents(1))
                                             .workflowState(SnapshotPolicy.afterEvents(1)));
            }));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 3);
            publishWorkflowWithEvents("wf-2", 3);

            // when
            load("wf-1");
            loadRunningWorkflows();

            // then
            assertThat(storedSnapshot("wf-1")).isNotNull();
            assertThat(storedRunningWorkflowsSnapshot()).isNotNull();
        }

        @Test
        void disabledConfigurationTakesNoSnapshot() {
            // given
            configuration = configuration(MODULE, configurer -> configurer.componentRegistry(cr -> {
                registerStore(cr);
                cr.registerComponent(WorkflowSnapshotConfiguration.class,
                                     c -> new WorkflowSnapshotConfiguration()
                                             .enabled(false)
                                             .runningWorkflows(SnapshotPolicy.afterEvents(1))
                                             .workflowState(SnapshotPolicy.afterEvents(1)));
            }));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 3);
            publishWorkflowWithEvents("wf-2", 3);

            // when
            load("wf-1");
            loadRunningWorkflows();

            // then
            assertThat(storedSnapshot("wf-1")).isNull();
            assertThat(storedRunningWorkflowsSnapshot()).isNull();
        }
    }

    @Nested
    class WhenSnapshotsAreConfiguredMoreThanOnce {

        @Test
        void disablingSnapshotsReplacesAnEarlierConfigurationEnablingThem() {
            // given
            configuration = configuration(MODULE, configurer -> configurer
                    .componentRegistry(cr -> registerStore(cr))
                    .snapshots(snapshots -> snapshots.enabled(true)
                                                     .runningWorkflows(SnapshotPolicy.afterEvents(1))
                                                     .workflowState(SnapshotPolicy.afterEvents(1)))
                    .snapshots(snapshots -> snapshots.enabled(false)));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 60);
            publishWorkflowWithEvents("wf-2", 3);

            // when
            load("wf-1");
            loadRunningWorkflows();

            // then
            assertThat(storedSnapshot("wf-1")).isNull();
            assertThat(storedRunningWorkflowsSnapshot()).isNull();
        }

        @Test
        void configurationLeavingSnapshotsUnsetReplacesAnEarlierConfigurationDisablingThem() {
            // given
            configuration = configuration(MODULE, configurer -> configurer
                    .componentRegistry(cr -> registerStore(cr))
                    .snapshots(snapshots -> snapshots.enabled(false))
                    .snapshots(snapshots -> snapshots.runningWorkflows(SnapshotPolicy.afterEvents(1))
                                                     .workflowState(SnapshotPolicy.afterEvents(1))));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 3);
            publishWorkflowWithEvents("wf-2", 3);

            // when
            load("wf-1");
            loadRunningWorkflows();

            // then - the default applies: only the state of each workflow instance is snapshotted
            assertThat(storedSnapshot("wf-1")).isNotNull();
            assertThat(storedRunningWorkflowsSnapshot()).isNull();
        }
    }

    @Nested
    class WhenSourcingFromASnapshot {

        @Test
        void loadStartsFromTheStoredSnapshotAndAppliesTheEventsAfterIt() {
            // given - a workflow whose state was snapshotted by its first load
            configuration = configuration(MODULE, configurer -> configurer.componentRegistry(cr -> registerStore(cr)));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 60);
            load("wf-1");
            var snapshot = Objects.requireNonNull(storedSnapshot("wf-1"));
            // and - that snapshot replaced by one holding a value that no event of the workflow leads to
            var json = new String(Objects.requireNonNull(converter.convert(snapshot.payload(), byte[].class)), UTF_8);
            assertThat(json).contains("\"step-0\":\"done\"");
            var alteredState = converter.convert(
                    json.replace("\"step-0\":\"done\"", "\"step-0\":\"from-snapshot\"").getBytes(UTF_8),
                    EventSourcedWorkflowState.class
            );
            FutureUtils.joinAndUnwrap(snapshotStore.store(
                    workflowStateSnapshotName(), "wf-1", snapshot.payload(Objects.requireNonNull(alteredState)), null
            ));
            // and - an event after the snapshot
            publishCompletedStep("wf-1", "step-after-snapshot");

            // when
            var state = load("wf-1");

            // then - the value only the snapshot holds shows the load started from the snapshot, not from the first
            // event, and the event after the snapshot is applied on top of it
            assertThat(state.payload()).containsEntry("step-0", "from-snapshot")
                                       .containsEntry("step-58", "done")
                                       .containsEntry("step-after-snapshot", "done");
            assertThat(state.workflowStepNames()).hasSize(60);
        }
    }

    @Nested
    class WhenStoringASnapshotFails {

        @Test
        void loadSucceedsWhenTheSnapshotStoreThrowsInsteadOfReturningAFailedFuture() {
            // given - a store that converts the snapshot before it returns a future, with a converter that cannot
            // convert the state, so it throws
            configuration = configuration(MODULE, configurer -> configurer
                    .componentRegistry(cr -> cr.registerComponent(
                            SnapshotStore.class, c -> new InMemorySnapshotStore(PassThroughConverter.INSTANCE)
                    ))
                    .snapshots(snapshots -> snapshots.enabled(true)
                                                     .runningWorkflows(SnapshotPolicy.afterEvents(1))
                                                     .workflowState(SnapshotPolicy.afterEvents(1))));
            configuration.start();
            publishWorkflowWithEvents("wf-1", 3);
            publishWorkflowWithEvents("wf-2", 3);

            // when
            var state = load("wf-1");
            var runningWorkflows = loadRunningWorkflows();

            // then - both loads succeed, without a snapshot
            assertThat(state.workflowStepNames()).hasSize(2);
            assertThat(runningWorkflows.workflowIds()).containsExactlyInAnyOrder("wf-1", "wf-2");
        }
    }

    private void registerStore(ComponentRegistry componentRegistry) {
        componentRegistry.registerComponent(SnapshotStore.class, c -> snapshotStore);
    }

    /**
     * Publishes the start of a workflow followed by completed steps, so its state is sourced from the given number of
     * events.
     */
    private void publishWorkflowWithEvents(String workflowId, int events) {
        var customizer = DefaultEventNameCustomizer.Builder.defaults();
        var context = workflowExecutionOperations(workflowId, DEFINITION_ID.version());
        publish(EventMessageUtils.startedWorkflow(context, "OrderWorkflow", DEFINITION_ID, customizer));
        for (int step = 0; step < events - 1; step++) {
            publishCompletedStep(workflowId, "step-" + step);
        }
    }

    private void publishCompletedStep(String workflowId, String stepName) {
        publish(EventMessageUtils.completedStep(
                workflowExecutionOperations(workflowId, DEFINITION_ID.version()), stepName,
                Map.of(stepName, "done"), CombineGlobalAndLocalPayloadReducer.NAME,
                DefaultEventNameCustomizer.Builder.defaults()
        ));
    }

    private EventSourcedWorkflowState load(String workflowId) {
        return load(EventSourcedWorkflowState.class, workflowId);
    }

    private EventSourcedRunningWorkflows loadRunningWorkflows() {
        return load(EventSourcedRunningWorkflows.class, EventSourcedRunningWorkflows.ENTITY_ID);
    }

    private <E> E load(Class<E> entityType, String identifier) {
        return configuration.getComponent(UnitOfWorkFactory.class)
                            .create("load-" + identifier)
                            .executeWithResult(context -> repository(entityType)
                                    .loadOrCreate(identifier, context)
                                    .thenApply(managedEntity -> managedEntity.entity()))
                            .orTimeout(5, TimeUnit.SECONDS)
                            .join();
    }

    /**
     * Returns the snapshot of the given workflow's state, stored under the name the default resolver gives the state.
     */
    @Nullable
    private Snapshot storedSnapshot(String workflowId) {
        return FutureUtils.joinAndUnwrap(snapshotStore.load(workflowStateSnapshotName(), workflowId, null));
    }

    /**
     * Returns the snapshot of the running-workflow list, stored under the name the application's resolver gives it.
     */
    @Nullable
    private Snapshot storedRunningWorkflowsSnapshot() {
        var snapshotName = configuration.getComponent(MessageTypeResolver.class)
                                        .resolveOrThrow(EventSourcedRunningWorkflows.class)
                                        .qualifiedName();
        return FutureUtils.joinAndUnwrap(
                snapshotStore.load(snapshotName, EventSourcedRunningWorkflows.ENTITY_ID, null)
        );
    }

    private static QualifiedName workflowStateSnapshotName() {
        return new AnnotationMessageTypeResolver().resolveOrThrow(EventSourcedWorkflowState.class).qualifiedName();
    }
}
