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

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.core.Message;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.*;

/**
 * Test class validating {@link DoNotEnqueue}. Constructs a {@code DoNotEnqueue} through the constructor and
 * {@link Decisions} utility class for testing.
 *
 * @author Steven van Beelen
 */
class DoNotEnqueueTest {

    private DeadLetter<EventMessage> testLetter;

    @BeforeEach
    void setUp() {
        GenericDeadLetter.clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        testLetter = new GenericDeadLetter<>("seqId", EventTestUtils.asEventMessage("payload"));
    }

    @Test
    void constructorDoNotEnqueueDoesNotAllowEnqueueing() {
        DoNotEnqueue<Message> testSubject = new DoNotEnqueue<>();

        assertThat(testSubject.shouldEnqueue()).isFalse();
        assertThat(testSubject.enqueueCause()).isEmpty();

        DeadLetter<? extends Message> result = testSubject.withDiagnostics(testLetter);
        assertThat(result).isEqualTo(testLetter);
    }

    @Test
    void decisionsDoNotEnqueueDoesNotAllowEnqueueing() {
        DoNotEnqueue<Message> testSubject = Decisions.doNotEnqueue();

        assertThat(testSubject.shouldEnqueue()).isFalse();
        assertThat(testSubject.enqueueCause()).isEmpty();

        DeadLetter<? extends Message> result = testSubject.withDiagnostics(testLetter);
        assertThat(result).isEqualTo(testLetter);
    }

    @Test
    void decisionsEvictDoesNotAllowEnqueueing() {
        DoNotEnqueue<Message> testSubject = Decisions.evict();

        assertThat(testSubject.shouldEnqueue()).isFalse();
        assertThat(testSubject.enqueueCause()).isEmpty();

        DeadLetter<? extends Message> result = testSubject.withDiagnostics(testLetter);
        assertThat(result).isEqualTo(testLetter);
    }
}