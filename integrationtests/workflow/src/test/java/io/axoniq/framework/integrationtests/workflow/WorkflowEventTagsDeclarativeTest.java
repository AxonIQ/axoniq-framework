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
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepTimedOutException;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.metadataProperty;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.*;
import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class WorkflowEventTagsDeclarativeTest extends AbstractWorkflowIntegrationTestBase<SimpleWorkflowContext> {

    WorkflowEventTagsDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        var workflow = new TagWorkflow();
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("Workflow tags")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.tags").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "tags-" + id))
                );
    }

    @Test
    void persistedEventsCanBeReadBackByWorkflowTags() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent("tagged-user", "tagged@test.com", "vip"))
        ));
        delayedPublisher.start();

        await().untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll().join()).hasSize(1);
            assertThat(workflowHistoryRepository.findAll().join())
                    .allMatch(h -> h.state().workflowStatus() == WorkflowStatus.COMPLETED);
            assertThat(workflowEngine.workflowExecutions()).isEmpty();
        });

        var workflowLifecycleEvents = eventsWithTag(Tag.of(TAG_WORKFLOW_EVENT_TYPE, TAG_VALUE_EVENT_TYPE_LIFECYCLE));
        var waitLifecycleEvents = eventsWithTag(Tag.of(TAG_WORKFLOW_EVENT_TYPE, TAG_VALUE_EVENT_TYPE_WAIT_STEP));
        var workflowEvents = eventsWithTag(Tag.of(TAG_WORKFLOW_ID, "tags-tagged-user"));

        assertThat(workflowLifecycleEvents).hasSize(2);
        assertThat(waitLifecycleEvents).hasSize(2);

        assertThat(workflowEvents)
                .extracting(event -> event.type().qualifiedName().toString())
                .containsExactly(
                        "io.axoniq.dsl.tags.WorkflowStarted",
                        "io.axoniq.dsl.tags.CooldownStarted",
                        "io.axoniq.dsl.tags.CooldownTimedOut",
                        "io.axoniq.dsl.tags.WorkflowCompleted"
                );
    }

    private List<EventMessage> eventsWithTag(Tag tag) {
        EventStorageEngine eventStorageEngine = configuration.getComponent(EventStorageEngine.class);
        MessageStream<EventMessage> stream = eventStorageEngine.source(
                SourcingCondition.conditionFor(EventCriteria.havingTags(tag))
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

    private static class TagWorkflow {

        public void execute(SimpleWorkflowContext ctx) {
            try {
                ctx.awaitEvent("cooldown",
                               SomethingHappened.class,
                               associate(payloadProperty("fact"), equalsTo("eclipse"))
                                       .and(metadataProperty("tenantId"), equalsTo("solar system")),
                               step -> step.timeout(Duration.ofMillis(50)));
            } catch (StepTimedOutException e) {
                // intentionally empty to just finish the workflow successfully
            }
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }

    private record SomethingHappened(
            String fact
    ) {

    }
}
