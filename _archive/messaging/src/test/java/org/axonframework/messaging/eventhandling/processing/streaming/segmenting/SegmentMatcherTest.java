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

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for {@link SegmentMatcher}.
 *
 * @author Mateusz Nowak
 */
class SegmentMatcherTest {

    @Test
    void matchesReturnsTrueWhenSegmentMatchesEventBasedOnSequenceIdentifier() {
        //given
        SegmentMatcher testSubject = new SegmentMatcher((message, context) -> Optional.of("sample-identifier"));
        EventMessage testMessage = EventTestUtils.asEventMessage("test-payload");
        Segment segment = new Segment(0, 0); // Root segment matches everything

        //when
        boolean result = testSubject.matches(segment, testMessage, new StubProcessingContext());

        //then
        assertThat(result).isTrue();
    }

    @Test
    void usesEventMessageIdentifierAsSequenceIdentifierWhenPolicyReturnsNull() {
        //given
        SegmentMatcher testSubject = new SegmentMatcher((message, context) -> Optional.empty());
        String messageId = UUID.randomUUID().toString();
        MessageType messageType = new MessageType(new QualifiedName(String.class));
        EventMessage testMessage = EventTestUtils.asEventMessage(
                new GenericEventMessage(messageId,
                                          messageType,
                                          "test-payload",
                                          Metadata.emptyInstance(),
                                          Instant.now()));
        Segment segment = Segment.ROOT_SEGMENT; // Matches everything

        //when
        boolean result = testSubject.matches(segment, testMessage, new StubProcessingContext());

        //then
        assertThat(result).isTrue();
    }

    @Test
    void matchesReturnsFalseWhenSegmentDoesNotMatchEventBasedOnSequenceIdentifier() {
        //given
        Segment segmentEven = new Segment(1, 1); // Will match events with odd hash
        String sequenceId = "even"; // "even" has a hash code of 3021508, which is even
        SegmentMatcher testSubject = new SegmentMatcher((message, context) -> Optional.of(sequenceId));
        EventMessage oddMessage = EventTestUtils.asEventMessage("test-payload");

        //when
        boolean result = testSubject.matches(segmentEven, oddMessage, new StubProcessingContext());

        //then
        assertThat(result).isFalse();
    }
}