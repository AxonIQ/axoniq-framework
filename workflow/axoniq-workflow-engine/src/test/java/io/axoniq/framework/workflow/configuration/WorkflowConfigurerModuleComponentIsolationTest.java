/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.EventCondition;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.WorkflowHistoryProjector;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager;
import io.axoniq.framework.workflow.runtime.execution.AbstractWorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.InMemoryWorkflowExecutionRepository;
import io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowExecutionRepository;
import org.awaitility.Awaitility;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test demonstrating that different modules use different components (global, local with history, local without
 * history), and that one module's {@link WorkflowEngine} never reacts to another module's workflow.
 *
 * @author Simon Zambrovski
 * @author Steven van Beelen
 */
class WorkflowConfigurerModuleComponentIsolationTest {

    private AxonConfiguration configuration;

    @AfterEach
    void tearDownConfiguration() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Test
    void moduleComponentIsolation() {
        WorkflowConfigurer configurer = WorkflowConfigurer.create();

        // 1. Global module (uses per-module defaults, no explicit override)
        var globalModule = WorkflowModule.defaults("global-module", TestContext.class)
                                         .contextFactory(c -> TestContext::new)
                                         .definition(d -> d.declarative(c -> (ctx) -> {
                                                           })
                                                           .workflowName("wf-global")
                                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName(
                                                                   "start")))
                                                           .notCustomized());

        // 2. Local module with history and explicit component overrides
        WorkflowConfigurationRegistry<?> localRegistryWithHistory = new SimpleWorkflowConfigurationRegistry();
        WorkflowExecutionRepository localRepositoryWithHistory = new InMemoryWorkflowExecutionRepository();
        MutableWorkflowHistoryRepository localHistoryRepository = new InMemoryWorkflowHistoryRepository();

        var localWithHistoryModule = WorkflowModule.configure("local-with-history-module", TestContext.class)
                                                   .configurationRegistry(cfg -> localRegistryWithHistory)
                                                   .executionRepository(cfg -> localRepositoryWithHistory)
                                                   .withHistory(cfg -> new WorkflowHistoryProjector(
                                                           localHistoryRepository
                                                   ))
                                                   .contextFactory(c -> TestContext::new)
                                                   .definition(d -> d.declarative(c -> (ctx) -> {
                                                                     })
                                                                     .workflowName("wf-local-history")
                                                                     .on(c -> EventConditions.fromQualifiedName(new QualifiedName(
                                                                             "start")))
                                                                     .notCustomized());

        // 3. Local module without history, also with explicit component overrides
        WorkflowConfigurationRegistry<?> localRegistryWithoutHistory = new SimpleWorkflowConfigurationRegistry();
        WorkflowExecutionRepository localRepositoryWithoutHistory = new InMemoryWorkflowExecutionRepository();

        var localWithoutHistoryModule = WorkflowModule.configure("local-without-history-module", TestContext.class)
                                                      .configurationRegistry(cfg -> localRegistryWithoutHistory)
                                                      .executionRepository(cfg -> localRepositoryWithoutHistory)
                                                      .withoutHistory()
                                                      .contextFactory(c -> TestContext::new)
                                                      .definition(d -> d.declarative(c -> (ctx) -> {
                                                                        })
                                                                        .workflowName("wf-local-no-history")
                                                                        .on(c -> EventConditions.fromQualifiedName(new QualifiedName(
                                                                                "start")))
                                                                        .notCustomized());

        configurer.componentRegistry(cr -> cr
                .registerModule(globalModule)
                .registerModule(localWithHistoryModule)
                .registerModule(localWithoutHistoryModule)
        );

        configuration = configurer.build();

        // then: each module resolves its own, distinctly named registry/repository/manager instance
        var registries = configuration.getComponents(WorkflowConfigurationRegistry.class);
        var repositories = configuration.getComponents(WorkflowExecutionRepository.class);
        var managers = configuration.getComponents(WorkflowManager.class);

        var globalRegistry = registries.get("WorkflowConfigurationRegistry[global-module]");
        var withHistoryRegistry = registries.get("WorkflowConfigurationRegistry[local-with-history-module]");
        var withoutHistoryRegistry = registries.get("WorkflowConfigurationRegistry[local-without-history-module]");
        var globalRepository = repositories.get("WorkflowExecutionRepository[global-module]");
        var withHistoryRepository = repositories.get("WorkflowExecutionRepository[local-with-history-module]");
        var withoutHistoryRepository = repositories.get("WorkflowExecutionRepository[local-without-history-module]");

        assertThat(globalRegistry).isNotNull();
        assertThat(withHistoryRegistry).isSameAs(localRegistryWithHistory);
        assertThat(withoutHistoryRegistry).isSameAs(localRegistryWithoutHistory);
        assertThat(globalRepository).isNotNull();
        assertThat(withHistoryRepository).isSameAs(localRepositoryWithHistory);
        assertThat(withoutHistoryRepository).isSameAs(localRepositoryWithoutHistory);

        // no two modules ever share the same registry/repository/manager instance
        assertThat(Set.of(globalRegistry, withHistoryRegistry, withoutHistoryRegistry)).hasSize(3);
        assertThat(Set.of(globalRepository, withHistoryRepository, withoutHistoryRepository)).hasSize(3);
        assertThat(managers.keySet()).containsExactlyInAnyOrder(
                "WorkflowManager[global-module]",
                "WorkflowManager[local-with-history-module]",
                "WorkflowManager[local-without-history-module]"
        );
        assertThat(Set.copyOf(managers.values())).hasSize(3);

        assertThat(configuration.getComponents(WorkflowEngine.class).get("WorkflowEngine[global-module]"))
                .isNotNull();
    }

    @Test
    void multipleModulesHaveDifferentEngines() {
        WorkflowConfigurer configurer = WorkflowConfigurer.create();

        EventCondition startCondition1 = EventConditions.fromQualifiedName(new QualifiedName("startEvent1"));
        WorkflowDefinition<TestContext1> definition1 = ctx -> {
        };

        EventCondition startCondition2 = EventConditions.fromQualifiedName(new QualifiedName("startEvent2"));
        WorkflowDefinition<TestContext2> definition2 = ctx -> {
        };

        var module1 = WorkflowModule.defaults("wf1", TestContext1.class)
                                    .contextFactory(c -> TestContext1::new)
                                    .definition(d -> d.declarative(c -> definition1)
                                                      .workflowName("wf1")
                                                      .on(c -> startCondition1)
                                                      .notCustomized());

        var module2 = WorkflowModule.defaults("wf2", TestContext2.class)
                                    .contextFactory(c -> TestContext2::new)
                                    .definition(d -> d.declarative(c -> definition2)
                                                      .workflowName("wf2")
                                                      .on(c -> startCondition2)
                                                      .notCustomized());

        configurer.componentRegistry(componentRegistry -> componentRegistry.registerModule(module1)
                                                                           .registerModule(module2));
        configuration = configurer.build();

        assertThat(configuration.getComponents(WorkflowEngine.class).get("WorkflowEngine[wf1]")).isNotNull();
        assertThat(configuration.getComponents(WorkflowEngine.class).get("WorkflowEngine[wf2]")).isNotNull();
    }

    @Test
    void engineDoesNotStartAnotherModulesWorkflow() {
        var storageEngine = new InMemoryEventStorageEngine();
        var moduleA = WorkflowModule.defaults("module-a", TestContext.class)
                                    .contextFactory(c -> TestContext::new)
                                    .definition(d -> d
                                            .declarative(c -> ctx -> {
                                            })
                                            .workflowName("wf-a")
                                            .on(c -> EventConditions.fromQualifiedName(new QualifiedName("startA")))
                                            .notCustomized()
                                    );
        var moduleB = WorkflowModule.defaults("module-b", TestContext.class)
                                    .contextFactory(c -> TestContext::new)
                                    .definition(d -> d
                                            .declarative(c -> ctx -> {
                                            })
                                            .workflowName("wf-b")
                                            .on(c -> EventConditions.fromQualifiedName(new QualifiedName("startB")))
                                            .notCustomized()
                                    );

        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> cr
                .registerComponent(EventStorageEngine.class, cfg -> storageEngine)
                .registerModule(moduleA)
                .registerModule(moduleB)
        );
        configuration = configurer.build();
        configuration.start();

        // when: only module A's start event is published
        var startEventForA = new GenericEventMessage(
                new MessageType(new QualifiedName("startA"), "0.0.1"),
                Map.<String, Object>of("orderId", "order-1"),
                Metadata.emptyInstance()
        );
        var workflowId = startEventForA.identifier();
        publish(startEventForA);

        // then: module A's own engine durably completed the workflow it owns
        Awaitility.await()
                  .atMost(Duration.ofSeconds(5))
                  .until(() -> durableStatus(workflowId) == WorkflowStatus.COMPLETED);
        // and: module B's engine never reacted to it - its own repository never held an execution for it
        assertThat(engineOf("module-b").workflowExecutions()).isEmpty();
    }

    private WorkflowEngine engineOf(String moduleName) {
        return configuration.getComponents(WorkflowEngine.class).get("WorkflowEngine[" + moduleName + "]");
    }

    @SuppressWarnings("unchecked")
    private WorkflowStatus durableStatus(String workflowId) {
        var repository = (Repository<String, EventSourcedWorkflowState>) configuration
                .getComponents(Repository.class)
                .values()
                .stream()
                .filter(candidate -> candidate.entityType().equals(EventSourcedWorkflowState.class))
                .findFirst()
                .orElseThrow();
        return configuration.getComponent(UnitOfWorkFactory.class)
                            .create("read-" + workflowId)
                            .executeWithResult(ctx -> repository.loadOrCreate(workflowId, ctx)
                                                                .thenApply(managed -> managed.entity()
                                                                                             .workflowStatus()))
                            .join();
    }

    private void publish(EventMessage eventMessage) {
        var eventStore = configuration.getComponent(EventStore.class);
        var unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        unitOfWorkFactory.create("publish-isolation-test")
                         .executeWithResult(context -> eventStore.publish(context, eventMessage)
                                                                 .thenApply(ignored -> eventMessage))
                         .join();
    }

    static class TestContext1 extends AbstractWorkflowContext {

        public TestContext1(Map<String, @Nullable Object> payload, String workflowId,
                            ProcessingContext processingContext,
                            WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }

    static class TestContext2 extends AbstractWorkflowContext {

        public TestContext2(Map<String, @Nullable Object> payload, String workflowId,
                            ProcessingContext processingContext,
                            WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }

    static class TestContext extends AbstractWorkflowContext {

        public TestContext(Map<String, @Nullable Object> payload, String workflowId,
                           ProcessingContext processingContext,
                           WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
