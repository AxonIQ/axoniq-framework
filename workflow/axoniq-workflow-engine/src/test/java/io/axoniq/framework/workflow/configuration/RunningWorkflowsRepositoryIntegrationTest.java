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

import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedRunningWorkflows;
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.TAG_VALUE_EVENT_TYPE_LIFECYCLE;
import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.TAG_WORKFLOW_EVENT_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * RunningWorkflows integration test.
 *
 * @author Simon Zambrovski
 */
class RunningWorkflowsRepositoryIntegrationTest extends AbstractEventSourcedEntityRepositoryTestBase {

    @Test
    void repositoryRebuildsRunningWorkflowIdsFromLifecycleTags() {
        configuration = configuration("running-workflows-module");
        configuration.start();

        var customizer = DefaultEventNameCustomizer.Builder.defaults();
        var definitionId = new MessageType(new QualifiedName("OrderWorkflow"), "0.0.1");
        var first = workflowContext("wf-1");
        var second = workflowContext("wf-2");

        publish(EventMessageUtils.startedWorkflow(first, "OrderWorkflow", definitionId, customizer));
        publish(EventMessageUtils.startedWorkflow(second, "OrderWorkflow", definitionId, customizer));
        publish(EventMessageUtils.completedWorkflow(first, "OrderWorkflow", definitionId, customizer));

        assertThat(lifecycleEvents()).hasSize(3);
        assertThat(load().workflowIds()).isEqualTo(Set.of("wf-2"));

        publish(EventMessageUtils.timeoutWorkflow(
                second,
                "OrderWorkflow",
                Instant.parse("2026-07-08T12:00:00Z"),
                definitionId,
                customizer
        ));

        assertThat(load().workflowIds()).isEmpty();
    }

    @Test
    void repositoryCreatesEmptyStateWhenNoLifecycleEventsExist() {
        configuration = configuration("running-workflows-module");
        configuration.start();

        assertThat(load().workflowIds()).isEmpty();
    }

    private EventSourcedRunningWorkflows load() {
        var unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        return unitOfWorkFactory.create("load-running-workflows-test")
                                .executeWithResult(context -> repository(EventSourcedRunningWorkflows.class)
                                        .loadOrCreate(
                                                EventSourcedRunningWorkflows.ENTITY_ID,
                                                context
                                        )
                                        .thenApply(managedEntity -> managedEntity.entity()))
                                .join();
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

}
