/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.util.Map;
import java.util.Set;

import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests how {@link EventSourcedWaitAssociations} follows the opening and closing of waits.
 *
 * @author Stefan Dragisic
 */
class EventSourcedWaitAssociationsTest {

    private static final String WORKFLOW_ID = "order-workflow-1";

    private static EventMessage event(Metadata metadata, Map<String, Object> payload) {
        return new GenericEventMessage(new MessageType(new QualifiedName("WorkflowEvent")), payload, metadata);
    }

    private static EventMessage waitStarted(String workflowId, String stepName, String awaited) {
        return event(MetadataUtils.markWaitForEventStep(MetadataUtils.create(workflowId, stepName, StepStatus.STARTED)),
                     Map.of("eventName", awaited, "associations", Set.of("payload:orderId=42")));
    }

    private static EventMessage stepEvent(String stepName, StepStatus status, boolean marked) {
        var metadata = MetadataUtils.create(WORKFLOW_ID, stepName, status);
        return event(marked ? MetadataUtils.markWaitForEventStep(metadata) : metadata, Map.of());
    }

    private static EventMessage lifecycle(WorkflowStatus status) {
        return event(MetadataUtils.create(WORKFLOW_ID, status), Map.of());
    }

    @Test
    void sourcesWaitStepAndLifecycleEvents() {
        assertThat(EventSourcedWaitAssociations.criteriaBuilder()).isEqualTo(EventCriteria.either(
                EventCriteria.havingTags(Tag.of(TAG_WORKFLOW_EVENT_TYPE, TAG_VALUE_EVENT_TYPE_WAIT_STEP)),
                EventCriteria.havingTags(Tag.of(TAG_WORKFLOW_EVENT_TYPE, TAG_VALUE_EVENT_TYPE_LIFECYCLE))
        ));
    }

    @Test
    void aStartedWaitAddsItsRow() {
        // given
        var waits = new EventSourcedWaitAssociations();

        // when
        waits.evolve(waitStarted(WORKFLOW_ID, "awaitPayment", "PaymentReceived"));

        // then
        assertThat(waits.waitAssociations()).containsExactly(new WaitAssociation(
                WORKFLOW_ID, "awaitPayment", new QualifiedName("PaymentReceived"), Set.of("payload:orderId=42")
        ));
    }

    @ParameterizedTest
    @EnumSource(value = StepStatus.class, names = {"COMPLETED", "TIMED_OUT", "CANCELLED"})
    void aTerminalWaitStepRemovesItsRow(StepStatus status) {
        // given
        var waits = new EventSourcedWaitAssociations();
        waits.evolve(waitStarted(WORKFLOW_ID, "awaitPayment", "PaymentReceived"));
        waits.evolve(waitStarted(WORKFLOW_ID, "awaitShipping", "OrderShipped"));

        // when
        waits.evolve(stepEvent("awaitPayment", status, true));

        // then
        assertThat(waits.waitAssociations()).extracting(WaitAssociation::stepName).containsExactly("awaitShipping");
    }

    @Test
    void aCancelledStepWithoutTheWaitMarkerStillRemovesTheRow() {
        // given: cancellations recorded before cancelled waits carried the marker
        var waits = new EventSourcedWaitAssociations();
        waits.evolve(waitStarted(WORKFLOW_ID, "awaitPayment", "PaymentReceived"));

        // when
        waits.evolve(stepEvent("awaitPayment", StepStatus.CANCELLED, false));

        // then
        assertThat(waits.waitAssociations()).isEmpty();
    }

    @Test
    void aStartedStepWithoutTheWaitMarkerAddsNoRow() {
        // given
        var waits = new EventSourcedWaitAssociations();

        // when
        waits.evolve(stepEvent("chargeCard", StepStatus.STARTED, false));

        // then
        assertThat(waits.waitAssociations()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(value = WorkflowStatus.class, names = {"COMPLETED", "FAILED", "TIMED_OUT", "CANCELLED"})
    void aTerminalWorkflowRemovesAllItsRows(WorkflowStatus status) {
        // given
        var waits = new EventSourcedWaitAssociations();
        waits.evolve(waitStarted(WORKFLOW_ID, "awaitPayment", "PaymentReceived"));
        waits.evolve(waitStarted(WORKFLOW_ID, "awaitShipping", "OrderShipped"));
        waits.evolve(waitStarted("order-workflow-2", "awaitPayment", "PaymentReceived"));

        // when
        waits.evolve(lifecycle(status));

        // then
        assertThat(waits.waitAssociations()).extracting(WaitAssociation::workflowId)
                                            .containsExactly("order-workflow-2");
    }

    @Test
    void aStartedWorkflowChangesNothing() {
        // given
        var waits = new EventSourcedWaitAssociations();
        waits.evolve(waitStarted(WORKFLOW_ID, "awaitPayment", "PaymentReceived"));

        // when
        waits.evolve(lifecycle(WorkflowStatus.STARTED));

        // then
        assertThat(waits.waitAssociations()).hasSize(1);
    }

    @Test
    void waitAssociationsReturnsAnImmutableCopy() {
        // given
        var waits = new EventSourcedWaitAssociations();
        waits.evolve(waitStarted(WORKFLOW_ID, "awaitPayment", "PaymentReceived"));

        // when
        var rows = waits.waitAssociations();
        waits.evolve(stepEvent("awaitPayment", StepStatus.COMPLETED, true));

        // then
        assertThat(rows).hasSize(1);
        assertThatThrownBy(rows::clear).isInstanceOf(UnsupportedOperationException.class);
    }
}
