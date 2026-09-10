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

import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
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

import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.TAG_WORKFLOW_ID;
import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static io.axoniq.framework.workflow.runtime.util.MetadataUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A workflow publishing a business event through {@code ctx.awaitPublish} appends exactly one event: the business event
 * itself, carrying the publisher's step metadata. That event starts a second workflow, which does not mistake the
 * publisher's step for its own.
 *
 * @author Stefan Dragisic
 */
class PublishWorkflowTest extends AbstractWorkflowIntegrationTestBase<SimpleWorkflowContext> {

    private static final String NAMESPACE = "io.axoniq.dsl.publish";
    private static final String APPROVAL_ID = "approval-order-1";
    private static final String SHIPMENT_ID = "shipment-order-1";

    PublishWorkflowTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        var approval = new ApprovalWorkflow();
        return d -> d
                .declarative(c -> approval::execute)
                .workflowName("Approval")
                .on(EventConditions.fromType(OrderPlaced.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace(NAMESPACE).workflowBaseName("Approval"))
                        .workflowIdProvider(fromPayloadAttribute(c, "orderId", id -> "approval-" + id))
                );
    }

    @Override
    protected List<Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>>> getAdditionalDefinitions() {
        var shipment = new ShipmentWorkflow();
        // A different id prefix: a start whose id already exists is rejected as a duplicate instance.
        return List.of(d -> d
                .declarative(c -> shipment::execute)
                .workflowName("Shipment")
                .on(EventConditions.fromType(OrderApproved.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace(NAMESPACE).workflowBaseName("Shipment"))
                        .workflowIdProvider(fromPayloadAttribute(c, "orderId", id -> "shipment-" + id))
                ));
    }

    @Test
    void publishAppendsOneBusinessEventThatIsTheStepAndStartsAnotherWorkflow() {
        // given
        delayedPublisher.addSchedules(List.of(ofMillis(100, new OrderPlaced("order-1"))));

        // when
        delayedPublisher.start();

        // then: both workflows complete
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var histories = workflowHistoryRepository.findAll();
            assertThat(histories).hasSize(2);
            assertThat(histories).allMatch(h -> h.state().workflowStatus() == WorkflowStatus.COMPLETED);
        });

        // the publisher records the published event as its completed step
        var approval = stateOf(APPROVAL_ID);
        assertThat(approval.workflowStepNames()).containsExactly("notifyApproved");
        assertThat(approval.getStep("notifyApproved").status()).isEqualTo(StepStatus.COMPLETED);

        // the started workflow does not register the publisher's step as its own
        var shipment = stateOf(SHIPMENT_ID);
        assertThat(shipment.workflowStepNames()).containsExactly("prepareShipment");
        assertThat(shipment.payload()).containsEntry("orderId", "order-1").containsEntry("approvedBy", "alice");

        // exactly one event for the step, with the user's type and payload
        var approvalEvents = eventsWithTag(Tag.of(TAG_WORKFLOW_ID, APPROVAL_ID));
        assertThat(approvalEvents)
                .extracting(event -> event.type().qualifiedName().toString())
                .containsExactly(NAMESPACE + ".ApprovalStarted", "io.acme.OrderApproved", NAMESPACE + ".ApprovalCompleted");
        var published = approvalEvents.get(1);
        assertThat(published.payloadAs(OrderApproved.class)).isEqualTo(new OrderApproved("order-1", "alice"));
        assertThat(published.metadata())
                .containsEntry(METADATA_KEY_WORKFLOW_ID, APPROVAL_ID)
                .containsEntry(METADATA_KEY_STEP_NAME, "notifyApproved")
                .containsEntry(METADATA_KEY_TYPE, StepStatus.COMPLETED.name())
                .containsEntry(METADATA_KEY_STEP_PRIMITIVE, STEP_PRIMITIVE_PUBLISH);
        // correlation data is inherited from the workflow's context, the same as on every other engine event
        assertThat(published.metadata().get("correlationId"))
                .isEqualTo(approvalEvents.get(0).metadata().get("correlationId"));
    }

    private WorkflowState stateOf(String workflowId) {
        return workflowHistoryRepository.findAll()
                                        .stream()
                                        .map(h -> h.state())
                                        .filter(s -> workflowId.equals(s.workflowId()))
                                        .findFirst()
                                        .orElseThrow(() -> new AssertionError("No history for " + workflowId));
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

    private static class ApprovalWorkflow {

        public void execute(SimpleWorkflowContext ctx) {
            var orderId = (String) ctx.workflowPayload().get("orderId");
            ctx.awaitPublish("notifyApproved", new OrderApproved(orderId, "alice"));
        }
    }

    private static class ShipmentWorkflow {

        public void execute(SimpleWorkflowContext ctx) {
            ctx.awaitExecute("prepareShipment", Boolean.class, () -> true);
        }
    }

    @Event(namespace = "io.acme", name = "OrderPlaced")
    public record OrderPlaced(String orderId) {
    }

    @Event(namespace = "io.acme", name = "OrderApproved")
    public record OrderApproved(String orderId, String approvedBy) {
    }
}
