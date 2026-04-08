/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.eventhandling.processing.streaming.segmenting;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.axonframework.messaging.eventhandling.replay.ResetContext;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for {@link SequenceOverridingEventHandlingComponent}.
 */
class SequenceOverridingEventHandlingComponentTest {

    @Test
    void sequenceIdentifierForUsesPolicyWhenItProvidesSequence() {
        // given
        var policySequenceId = "policy-sequence-id";
        var delegateSequenceId = "delegate-sequence-id";
        SequencingPolicy<EventMessage> policy = (event, context) -> Optional.of(policySequenceId);
        var delegate = getEventHandlingComponentWithSequenceId(delegateSequenceId);
        var testSubject = new SequenceOverridingEventHandlingComponent(policy, delegate);
        var testEvent = new GenericEventMessage(
                new MessageType("TestEvent"),
                "test-payload"
        );

        // when
        var result = testSubject.sequenceIdentifierFor(testEvent, new StubProcessingContext());

        // then
        assertThat(result).isEqualTo(policySequenceId);
    }

    @Test
    void sequenceIdentifierForUsesDelegateWhenPolicyReturnsEmpty() {
        // given
        var delegateSequenceId = "delegate-sequence-id";
        SequencingPolicy<EventMessage> policy = (event, context) -> Optional.empty();
        EventHandlingComponent delegate = getEventHandlingComponentWithSequenceId(delegateSequenceId);
        var testSubject = new SequenceOverridingEventHandlingComponent(policy, delegate);
        var testEvent = new GenericEventMessage(
                new MessageType("TestEvent"),
                "test-payload"
        );

        // when
        var result = testSubject.sequenceIdentifierFor(testEvent, new StubProcessingContext());

        // then
        assertThat(result).isEqualTo(delegateSequenceId);
    }

    private @NonNull EventHandlingComponent getEventHandlingComponentWithSequenceId(String delegateSequenceId) {
        return new EventHandlingComponent() {
            @NonNull
            @Override
            public Object sequenceIdentifierFor(@NonNull EventMessage event, @NonNull ProcessingContext context) {
                return delegateSequenceId;
            }

            @NonNull
            @Override
            public Set<QualifiedName> supportedEvents() {
                return Set.of();
            }

            @Override
            public MessageStream.@NonNull Empty<Message> handle(@NonNull EventMessage event,
                                                                @NonNull ProcessingContext context) {
                return MessageStream.empty();
            }

            @Override
            public void describeTo(@NonNull ComponentDescriptor descriptor) {
                // Not important for this test to implement
            }

            @Override
            public MessageStream.@NonNull Empty<Message> handle(@NonNull ResetContext resetContext,
                                                                @NonNull ProcessingContext context) {
                return MessageStream.empty();
            }

            @Override
            public MessageStream.@NonNull Empty<Message> handle(@NonNull ReplayStatusChanged statusChange,
                                                                @NonNull ProcessingContext context) {
                return MessageStream.empty();
            }
        };
    }
}
