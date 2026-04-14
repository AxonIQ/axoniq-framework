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

package io.axoniq.framework.messaging.deadletter;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.*;

/**
 * Test class validating the {@link InMemorySequencedDeadLetterQueue}.
 *
 * @author Steven van Beelen
 * @author Mateusz Nowak
 * @since 5.0.0
 */
class InMemorySequencedDeadLetterQueueTest extends SequencedDeadLetterQueueTest<EventMessage> {

    private static final int MAX_SEQUENCES_AND_SEQUENCE_SIZE = 128;

    private final AtomicLong sequenceCounter = new AtomicLong(0);

    @Override
    protected SequencedDeadLetterQueue<EventMessage> buildTestSubject() {
        return InMemorySequencedDeadLetterQueue.<EventMessage>builder()
                                               .maxSequences(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                                               .maxSequenceSize(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                                               .build();
    }

    @Override
    protected long maxSequences() {
        return MAX_SEQUENCES_AND_SEQUENCE_SIZE;
    }

    @Override
    protected long maxSequenceSize() {
        return MAX_SEQUENCES_AND_SEQUENCE_SIZE;
    }

    @Override
    public DeadLetter<EventMessage> generateInitialLetter() {
        return new GenericDeadLetter<>("sequenceIdentifier", generateEvent(), generateThrowable(),
                                      buildTestContext());
    }

    @Override
    protected DeadLetter<EventMessage> generateFollowUpLetter() {
        return new GenericDeadLetter<>("sequenceIdentifier", generateEvent(), (Throwable) null,
                                      buildTestContext());
    }

    private Context buildTestContext() {
        long seqNo = sequenceCounter.getAndIncrement();
        return Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(seqNo))
                      .withResource(LegacyResources.AGGREGATE_TYPE_KEY, "TestAggregate")
                      .withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "aggregate-" + seqNo)
                      .withResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY, seqNo);
    }

    @Override
    protected DeadLetter<EventMessage> generateRequeuedLetter(DeadLetter<EventMessage> original,
                                                              Instant lastTouched,
                                                              Throwable requeueCause,
                                                              Metadata diagnostics) {
        setAndGetTime(lastTouched);
        return original.withCause(requeueCause)
                       .withDiagnostics(diagnostics)
                       .markTouched();
    }

    @Override
    protected void setClock(Clock clock) {
        GenericDeadLetter.clock = clock;
    }

    @Nested
    class WhenBuilding {

        @Test
        void buildDefaultQueue() {
            // when / then
            assertThatCode(InMemorySequencedDeadLetterQueue::defaultQueue).doesNotThrowAnyException();
        }

        @Test
        void buildWithNegativeMaxSequencesThrowsAxonConfigurationException() {
            // given
            InMemorySequencedDeadLetterQueue.Builder<EventMessage> builderTestSubject =
                    InMemorySequencedDeadLetterQueue.builder();

            // when / then
            assertThatThrownBy(() -> builderTestSubject.maxSequences(-1)).isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void buildWithZeroMaxSequencesThrowsAxonConfigurationException() {
            // given
            InMemorySequencedDeadLetterQueue.Builder<EventMessage> builderTestSubject =
                    InMemorySequencedDeadLetterQueue.builder();

            // when / then
            assertThatThrownBy(() -> builderTestSubject.maxSequences(0)).isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void buildWithNegativeMaxSequenceSizeThrowsAxonConfigurationException() {
            // given
            InMemorySequencedDeadLetterQueue.Builder<EventMessage> builderTestSubject =
                    InMemorySequencedDeadLetterQueue.builder();

            // when / then
            assertThatThrownBy(() -> builderTestSubject.maxSequenceSize(-1)).isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void buildWithZeroMaxSequenceSizeThrowsAxonConfigurationException() {
            // given
            InMemorySequencedDeadLetterQueue.Builder<EventMessage> builderTestSubject =
                    InMemorySequencedDeadLetterQueue.builder();

            // when / then
            assertThatThrownBy(() -> builderTestSubject.maxSequenceSize(0)).isInstanceOf(AxonConfigurationException.class);
        }
    }
}
