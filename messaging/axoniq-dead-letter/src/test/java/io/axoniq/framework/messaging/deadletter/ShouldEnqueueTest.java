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

import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.Metadata;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * Test class validating {@link ShouldEnqueue}.  Constructs a {@code ShouldEnqueue} through the constructor and
 * {@link Decisions} utility class for testing.
 *
 * @author Steven van Beelen
 */
class ShouldEnqueueTest {

    private DeadLetter<? extends Message> testLetter;

    @BeforeEach
    void setUp() {
        GenericDeadLetter.clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        testLetter = new GenericDeadLetter<>("seqId", EventTestUtils.asEventMessage("payload"));
    }

    @Test
    void constructorShouldEnqueueAllowsEnqueueing() {
        ShouldEnqueue<Message> testSubject = new ShouldEnqueue<>();

        assertThat(testSubject.shouldEnqueue()).isTrue();
        assertThat(testSubject.enqueueCause()).isEmpty();

        DeadLetter<? extends Message> result = testSubject.withDiagnostics(testLetter);
        assertThat(result).isEqualTo(testLetter);
    }

    @Test
    void decisionsEnqueueAllowsEnqueueing() {
        ShouldEnqueue<Message> testSubject = Decisions.enqueue();

        assertThat(testSubject.shouldEnqueue()).isTrue();
        assertThat(testSubject.enqueueCause()).isEmpty();

        DeadLetter<? extends Message> result = testSubject.withDiagnostics(testLetter);
        assertThat(result).isEqualTo(testLetter);
    }

    @Test
    void constructorShouldEnqueueWithCauseAllowsEnqueueingWithGivenCause() {
        Throwable testCause = new RuntimeException("just because");

        ShouldEnqueue<Message> testSubject = new ShouldEnqueue<>(testCause);

        assertThat(testSubject.shouldEnqueue()).isTrue();
        Optional<Throwable> resultCause = testSubject.enqueueCause();
        assertThat(resultCause)
                .isPresent()
                .hasValue(testCause);

        DeadLetter<? extends Message> result = testSubject.withDiagnostics(testLetter);
        assertThat(result).isEqualTo(testLetter);
    }

    @Test
    void decisionsEnqueueWithCauseAllowsEnqueueingWithGivenCause() {
        Throwable testCause = new RuntimeException("just because");

        ShouldEnqueue<Message> testSubject = Decisions.enqueue(testCause);

        assertThat(testSubject.shouldEnqueue()).isTrue();
        Optional<Throwable> resultCause = testSubject.enqueueCause();
        assertThat(resultCause)
                .isPresent()
                .hasValue(testCause);

        DeadLetter<? extends Message> result = testSubject.withDiagnostics(testLetter);
        assertThat(result).isEqualTo(testLetter);
    }

    @Test
    void decisionsRequeueWithCauseAllowsEnqueueingWithGivenCause() {
        Throwable testCause = new RuntimeException("just because");

        ShouldEnqueue<Message> testSubject = Decisions.requeue(testCause);

        assertThat(testSubject.shouldEnqueue()).isTrue();
        Optional<Throwable> resultCause = testSubject.enqueueCause();
        assertThat(resultCause)
                .isPresent()
                .hasValue(testCause);

        DeadLetter<? extends Message> result = testSubject.withDiagnostics(testLetter);
        assertThat(result).isEqualTo(testLetter);
    }

    @Test
    void constructorShouldEnqueueWithCauseAndDiagnosticsAllowsEnqueueingWithGivenCauseAndDiagnostics() {
        Throwable testCause = new RuntimeException("just because");
        Metadata testMetadata = Metadata.with("key", "value");

        ShouldEnqueue<Message> testSubject = new ShouldEnqueue<>(testCause, letter -> testMetadata);

        assertThat(testSubject.shouldEnqueue()).isTrue();
        Optional<Throwable> resultCause = testSubject.enqueueCause();
        assertThat(resultCause)
                .isPresent()
                .hasValue(testCause);

        DeadLetter<? extends Message> result = testSubject.withDiagnostics(testLetter);
        assertThat(result.message()).isEqualTo(testLetter.message());
        assertThat(result.cause()).isEqualTo(testLetter.cause());
        assertThat(result.enqueuedAt()).isEqualTo(testLetter.enqueuedAt());
        assertThat(result.lastTouched()).isEqualTo(testLetter.lastTouched());
        assertThat(result.diagnostics()).isEqualTo(testMetadata);
    }

    @Test
    void decisionsRequeueWithCauseAndDiagnosticsAllowsEnqueueingWithGivenCauseAndDiagnostics() {
        Throwable testCause = new RuntimeException("just because");
        Metadata testMetadata = Metadata.with("key", "value");

        ShouldEnqueue<Message> testSubject = Decisions.requeue(testCause, letter -> testMetadata);

        assertThat(testSubject.shouldEnqueue()).isTrue();
        Optional<Throwable> resultCause = testSubject.enqueueCause();
        assertThat(resultCause)
                .isPresent()
                .hasValue(testCause);

        DeadLetter<? extends Message> result = testSubject.withDiagnostics(testLetter);
        assertThat(result.message()).isEqualTo(testLetter.message());
        assertThat(result.cause()).isEqualTo(testLetter.cause());
        assertThat(result.enqueuedAt()).isEqualTo(testLetter.enqueuedAt());
        assertThat(result.lastTouched()).isEqualTo(testLetter.lastTouched());
        assertThat(result.diagnostics()).isEqualTo(testMetadata);
    }
}