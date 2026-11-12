/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.*;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests building a {@link WaitAssociation} from a wait-step STARTED event.
 *
 * @author Stefan Dragisic
 */
class WaitAssociationTest {

    private static final String WORKFLOW_ID = "order-workflow-1";
    private static final String STEP_NAME = "awaitPayment";

    private static EventMessage startedWait(Object payload) {
        return new GenericEventMessage(
                new MessageType(new QualifiedName("AwaitPaymentStarted")),
                payload,
                MetadataUtils.markWaitForEventStep(MetadataUtils.create(WORKFLOW_ID, STEP_NAME, StepStatus.STARTED))
        );
    }

    @Nested
    class FromStartedEvent {

        @Test
        void readsTheAwaitedEventNameAndAllAssociations() {
            // given
            var event = startedWait(Map.of(
                    "eventName", "PaymentReceived",
                    "associations", Set.of("payload:orderId=42", "metadata:tenant=acme")
            ));

            // when
            var row = WaitAssociation.fromStartedEvent(event);

            // then
            assertThat(row.workflowId()).isEqualTo(WORKFLOW_ID);
            assertThat(row.stepName()).isEqualTo(STEP_NAME);
            assertThat(row.eventName()).isEqualTo(new QualifiedName("PaymentReceived"));
            assertThat(row.associations()).containsExactlyInAnyOrder("payload:orderId=42", "metadata:tenant=acme");
            assertThat(row.isWildcard()).isFalse();
        }

        @Test
        void acceptsAssociationsAsAListAfterAPayloadRoundTrip() {
            // given: a converter turns the stored set into a list
            var event = startedWait(Map.of("eventName", "PaymentReceived",
                                           "associations", List.of("payload:orderId=42")));

            // when
            var row = WaitAssociation.fromStartedEvent(event);

            // then
            assertThat(row.associations()).containsExactly("payload:orderId=42");
        }

        @Test
        void aSleepWaitsForVoidWithoutAssociations() {
            // given
            var event = startedWait(Map.of("eventName", new QualifiedName(Void.class).toString(),
                                           "associations", Set.of()));

            // when
            var row = WaitAssociation.fromStartedEvent(event);

            // then
            assertThat(row.eventName()).isEqualTo(new QualifiedName(Void.class));
            assertThat(row.associations()).isEmpty();
        }

        @Test
        void unreadableAssociationsMatchOnTheEventNameAlone() {
            // given
            var event = startedWait(Map.of("eventName", "PaymentReceived", "associations", "not-a-collection"));

            // when
            var row = WaitAssociation.fromStartedEvent(event);

            // then
            assertThat(row.eventName()).isEqualTo(new QualifiedName("PaymentReceived"));
            assertThat(row.associations()).isEmpty();
        }

        @Test
        void aPayloadWithoutAnEventNameGivesAWildcardRow() {
            // given
            var event = startedWait(Map.of("associations", Set.of("payload:orderId=42")));

            // when
            var row = WaitAssociation.fromStartedEvent(event);

            // then
            assertThat(row.isWildcard()).isTrue();
            assertThat(row.eventName()).isNull();
            assertThat(row.associations()).isEmpty();
        }

        @Test
        void anUnreadablePayloadGivesAWildcardRowInsteadOfFailing() {
            // given: a payload that cannot be read as a map without a converter
            var event = startedWait("not a map");

            // when
            var row = WaitAssociation.fromStartedEvent(event);

            // then
            assertThat(row.isWildcard()).isTrue();
            assertThat(row.workflowId()).isEqualTo(WORKFLOW_ID);
            assertThat(row.stepName()).isEqualTo(STEP_NAME);
        }
    }

    @Test
    void associationsAreCopiedAndImmutable() {
        // given
        var associations = new HashSet<>(Set.of("payload:orderId=42"));
        var row = new WaitAssociation(WORKFLOW_ID, STEP_NAME, new QualifiedName("PaymentReceived"), associations);

        // when
        associations.add("payload:orderId=43");

        // then
        assertThat(row.associations()).containsExactly("payload:orderId=42");
        assertThatThrownBy(() -> row.associations().add("payload:orderId=44"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
