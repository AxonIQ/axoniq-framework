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

package org.axonframework.messaging.core.sequencing;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link HierarchicalSequencingPolicy}.
 *
 * @author Mateusz Nowak
 */
final class HierarchicalSequencingPolicyTest {

    @Nested
    class Construction {

        @Test
        void shouldThrowNullPointerExceptionWhenPrimaryIsNull() {
            // given
            SequencingPolicy<Message> secondary = (event, context) -> Optional.of("secondary");

            // when / then
            assertThrows(NullPointerException.class, () -> new HierarchicalSequencingPolicy<>(null, secondary));
        }

        @Test
        void shouldThrowNullPointerExceptionWhenSecondaryIsNull() {
            // given
            SequencingPolicy<Message> primary = (event, context) -> Optional.of("primary");

            // when / then
            assertThrows(NullPointerException.class, () -> new HierarchicalSequencingPolicy<>(primary, null));
        }
    }

    @Nested
    class SequenceIdentification {

        @Test
        void shouldUsePrimaryWhenPrimarySucceeds() {
            // given
            var expectedIdentifier = "primary-result";
            SequencingPolicy<Message> primary = (event, context) -> Optional.of(expectedIdentifier);
            SequencingPolicy<Message> secondary = (event, context) -> Optional.of("secondary-result");
            HierarchicalSequencingPolicy<Message> policy = new HierarchicalSequencingPolicy<>(primary, secondary);

            // when
            var result = policy.sequenceIdentifierFor(anEvent("test"), aProcessingContext());

            // then
            assertThat(result).hasValue(expectedIdentifier);
        }

        @Test
        void shouldUseSecondaryWhenPrimaryReturnsEmpty() {
            // given
            var expectedIdentifier = "secondary-result";
            SequencingPolicy<Message> primary = (event, context) -> Optional.empty();
            SequencingPolicy<Message> secondary = (event, context) -> Optional.of(expectedIdentifier);
            HierarchicalSequencingPolicy<Message> policy = new HierarchicalSequencingPolicy<>(primary, secondary);

            // when
            var result = policy.sequenceIdentifierFor(anEvent("test"), aProcessingContext());

            // then
            assertThat(result).hasValue(expectedIdentifier);
        }

        @Test
        void shouldReturnEmptyWhenBothReturnEmpty() {
            // given
            SequencingPolicy<Message> primary = (event, context) -> Optional.empty();
            SequencingPolicy<Message> secondary = (event, context) -> Optional.empty();
            HierarchicalSequencingPolicy<Message> policy = new HierarchicalSequencingPolicy<>(primary, secondary);

            // when
            var result = policy.sequenceIdentifierFor(anEvent("test"), aProcessingContext());

            // then
            assertThat(result).isEmpty();
        }
    }

    private EventMessage anEvent(final Object payload) {
        return EventTestUtils.asEventMessage(payload);
    }

    private static ProcessingContext aProcessingContext() {
        return new StubProcessingContext();
    }
}