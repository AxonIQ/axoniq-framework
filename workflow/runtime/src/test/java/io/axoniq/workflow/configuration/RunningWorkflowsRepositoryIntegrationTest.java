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
package io.axoniq.workflow.configuration;

import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.execution.AbstractDSLWorkflowContext;
import io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.execution.RunningWorkflows;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.axonframework.modelling.repository.Repository;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.axoniq.workflow.runtime.execution.WorkflowEventTags.TAG_VALUE_EVENT_TYPE_LIFECYCLE;
import static io.axoniq.workflow.runtime.execution.WorkflowEventTags.TAG_WORKFLOW_EVENT_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * RunningWorkflows integration test.
 *
 * @author Simon Zambrovski
 */
class RunningWorkflowsRepositoryIntegrationTest {

    private AxonConfiguration configuration;

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Test
    void repositoryRebuildsRunningWorkflowIdsFromLifecycleTags() {
        configuration = configuration();
        configuration.start();

        var customizer = DefaultEventNameCustomizer.Builder.defaults();
        var first = workflowContext("wf-1");
        var second = workflowContext("wf-2");

        publish(EventMessageUtils.startedWorkflow(first, "OrderWorkflow", customizer));
        publish(EventMessageUtils.startedWorkflow(second, "OrderWorkflow", customizer));
        publish(EventMessageUtils.completedWorkflow(first, "OrderWorkflow", customizer));

        assertThat(lifecycleEvents()).hasSize(3);
        assertThat(load().workflowIds()).isEqualTo(Set.of("wf-2"));

        publish(EventMessageUtils.timeoutWorkflow(
                second,
                "OrderWorkflow",
                Instant.parse("2026-07-08T12:00:00Z"),
                customizer
        ));

        assertThat(load().workflowIds()).isEmpty();
    }

    @Test
    void repositoryCreatesEmptyStateWhenNoLifecycleEventsExist() {
        configuration = configuration();
        configuration.start();

        assertThat(load().workflowIds()).isEmpty();
    }

    private AxonConfiguration configuration() {
        var module = WorkflowModule.defaults("running-workflows-module", TestContext.class)
                                   .workflowContextFactory(c -> TestContext::new)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> {
                                           })
                                           .workflowName("running-workflows")
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   );

        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> cr.registerModule(module));
        return configurer.build();
    }

    private void publish(EventMessage eventMessage) {
        var eventStore = configuration.getComponent(EventStore.class);
        var unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        unitOfWorkFactory.create("publish-running-workflows-test")
                         .executeWithResult(context -> eventStore.publish(context, eventMessage)
                                                                 .thenApply(ignored -> eventMessage))
                         .join();
    }

    private RunningWorkflows load() {
        var unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        return unitOfWorkFactory.create("load-running-workflows-test")
                                .executeWithResult(context -> repository()
                                        .loadOrCreate(
                                                RunningWorkflows.ENTITY_ID,
                                                context
                                        )
                                        .thenApply(managedEntity -> managedEntity.entity()))
                                .join();
    }

    @SuppressWarnings("unchecked")
    private Repository<String, RunningWorkflows> repository() {
        return configuration.getComponents(Repository.class)
                            .values()
                            .stream()
                            .filter(repository -> repository.entityType().equals(RunningWorkflows.class))
                            .map(repository -> (Repository<String, RunningWorkflows>) repository)
                            .findFirst()
                            .orElseThrow();
    }

    private List<EventMessage> lifecycleEvents() {
        var eventStorageEngine = configuration.getComponent(EventStorageEngine.class);
        MessageStream<EventMessage> stream = eventStorageEngine.source(
                SourcingCondition.conditionFor(
                        EventCriteria.havingTags(Tag.of(
                                TAG_WORKFLOW_EVENT_TYPE,
                                TAG_VALUE_EVENT_TYPE_LIFECYCLE
                        ))
                )
        );
        try {
            return stream.reduce(new ArrayList<EventMessage>(), (events, entry) -> {
                EventMessage event = entry.message();
                if (!(event instanceof TerminalEventMessage)) {
                    events.add(event);
                }
                return events;
            }).join();
        } finally {
            stream.close();
        }
    }

    private static WorkflowContext workflowContext(String workflowId) {
        var context = mock(WorkflowContext.class);
        when(context.workflowId()).thenReturn(workflowId);
        when(context.workflowPayload()).thenReturn(Map.of("orderId", workflowId));
        when(context.workflowVersion()).thenReturn("0.0.1");
        return context;
    }

    static class TestContext extends AbstractDSLWorkflowContext {

        public TestContext(@Nonnull Map<String, Object> payload, @Nonnull String workflowId,
                           @Nonnull ProcessingContext processingContext,
                           @Nonnull WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
