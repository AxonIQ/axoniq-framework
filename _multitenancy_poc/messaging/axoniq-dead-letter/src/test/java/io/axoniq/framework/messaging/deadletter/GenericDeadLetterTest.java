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
import org.axonframework.messaging.core.Metadata;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * Test class validating the {@link GenericDeadLetter}.
 *
 * @author Steven van Beelen
 */
class GenericDeadLetterTest {

    private static final String SEQUENCE_IDENTIFIER = "sesequenceIdentifier";
    private static final EventMessage MESSAGE = EventTestUtils.asEventMessage("payload");

    @Test
    void constructorForIdentifierAndMessageSetsGivenIdentifierAndMessage() {
        Instant expectedTime = Instant.now();
        GenericDeadLetter.clock = Clock.fixed(expectedTime, ZoneId.systemDefault());

        DeadLetter<EventMessage> testSubject = new GenericDeadLetter<>(SEQUENCE_IDENTIFIER, MESSAGE);

        assertThat(testSubject.message()).isEqualTo(MESSAGE);
        assertThat(testSubject.cause().isPresent()).isFalse();
        assertThat(testSubject.enqueuedAt()).isEqualTo(expectedTime);
        assertThat(testSubject.lastTouched()).isEqualTo(expectedTime);
        assertThat(testSubject.diagnostics().isEmpty()).isTrue();
    }

    @Test
    void constructorForIdentifierMessageAndThrowableSetsGivenIdentifierMessageAndIdentifier() {
        Throwable testThrowable = new RuntimeException("just because");
        ThrowableCause expectedCause = new ThrowableCause(testThrowable);
        Instant expectedTime = Instant.now();
        GenericDeadLetter.clock = Clock.fixed(expectedTime, ZoneId.systemDefault());

        DeadLetter<EventMessage> testSubject =
                new GenericDeadLetter<>(SEQUENCE_IDENTIFIER, MESSAGE, testThrowable);

        assertThat(testSubject.message()).isEqualTo(MESSAGE);
        Optional<Cause> resultCause = testSubject.cause();
        assertThat(resultCause.isPresent()).isTrue();
        assertThat(resultCause.get()).isEqualTo(expectedCause);
        assertThat(testSubject.enqueuedAt()).isEqualTo(expectedTime);
        assertThat(testSubject.lastTouched()).isEqualTo(expectedTime);
        assertThat(testSubject.diagnostics().isEmpty()).isTrue();
    }

    @Test
    void constructorCompletelyManualSetsGivenFields() {
        ThrowableCause expectedCause = new ThrowableCause(new RuntimeException("just because"));
        Instant expectedEnqueuedAt = Instant.now();
        Instant expectedLastTouched = Instant.now();
        Metadata expectedDiagnostics = Metadata.with("key", "value");

        DeadLetter<EventMessage> testSubject = new GenericDeadLetter<>(
                SEQUENCE_IDENTIFIER, MESSAGE, expectedCause, expectedEnqueuedAt, expectedLastTouched,
                expectedDiagnostics
        );

        assertThat(testSubject.message()).isEqualTo(MESSAGE);
        Optional<Cause> resultCause = testSubject.cause();
        assertThat(resultCause.isPresent()).isTrue();
        assertThat(resultCause.get()).isEqualTo(expectedCause);
        assertThat(testSubject.enqueuedAt()).isEqualTo(expectedEnqueuedAt);
        assertThat(testSubject.lastTouched()).isEqualTo(expectedLastTouched);
        assertThat(testSubject.diagnostics()).isEqualTo(expectedDiagnostics);
    }

    @Test
    void invokingMarkTouchedAdjustsLastTouched() {
        DeadLetter<EventMessage> testSubject = new GenericDeadLetter<>(SEQUENCE_IDENTIFIER, MESSAGE);

        Instant expectedLastTouched = Instant.now();
        GenericDeadLetter.clock = Clock.fixed(expectedLastTouched, ZoneId.systemDefault());
        DeadLetter<EventMessage> result = testSubject.markTouched();

        assertThat(result.message()).isEqualTo(testSubject.message());
        Optional<Cause> resultCause = result.cause();
        assertThat(resultCause.isPresent()).isFalse();
        assertThat(result.enqueuedAt()).isEqualTo(testSubject.enqueuedAt());
        assertThat(result.lastTouched()).isEqualTo(expectedLastTouched);
        assertThat(result.diagnostics()).isEqualTo(testSubject.diagnostics());
    }

    @Test
    void invokingWithCauseAndWithoutOriginalCauseSetsGivenCause() {
        // Fix the clock to keep time consistent after withCause invocation.
        GenericDeadLetter.clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        Throwable testThrowable = new RuntimeException("just because");
        ThrowableCause expectedCause = new ThrowableCause(testThrowable);

        DeadLetter<EventMessage> testSubject = new GenericDeadLetter<>(SEQUENCE_IDENTIFIER, MESSAGE);

        DeadLetter<EventMessage> result = testSubject.withCause(testThrowable);

        assertThat(result.message()).isEqualTo(testSubject.message());
        Optional<Cause> resultCause = result.cause();
        assertThat(resultCause.isPresent()).isTrue();
        assertThat(resultCause.get()).isEqualTo(expectedCause);
        assertThat(result.enqueuedAt()).isEqualTo(testSubject.enqueuedAt());
        assertThat(result.lastTouched()).isEqualTo(testSubject.lastTouched());
        assertThat(result.diagnostics()).isEqualTo(testSubject.diagnostics());
    }

    @Test
    void invokingWithCauseAndOriginalCauseReplacesTheOriginalCause() {
        // Fix the clock to keep time consistent after withCause invocation.
        GenericDeadLetter.clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        Throwable originalThrowable = new RuntimeException("some other issue");
        Throwable testThrowable = new RuntimeException("just because");
        ThrowableCause expectedCause = new ThrowableCause(testThrowable);

        DeadLetter<EventMessage> testSubject =
                new GenericDeadLetter<>(SEQUENCE_IDENTIFIER, MESSAGE, originalThrowable);

        DeadLetter<EventMessage> result = testSubject.withCause(testThrowable);

        assertThat(result.message()).isEqualTo(testSubject.message());
        Optional<Cause> resultCause = result.cause();
        assertThat(resultCause.isPresent()).isTrue();
        assertThat(resultCause.get()).isEqualTo(expectedCause);
        assertThat(result.enqueuedAt()).isEqualTo(testSubject.enqueuedAt());
        assertThat(result.lastTouched()).isEqualTo(testSubject.lastTouched());
        assertThat(result.diagnostics()).isEqualTo(testSubject.diagnostics());
    }

    @Test
    void invokingWithNullCauseAndNoOriginalCauseLeavesTheCauseEmpty() {
        // Fix the clock to keep time consistent after withCause invocation.
        GenericDeadLetter.clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        DeadLetter<EventMessage> testSubject = new GenericDeadLetter<>(SEQUENCE_IDENTIFIER, MESSAGE);

        DeadLetter<EventMessage> result = testSubject.withCause(null);

        assertThat(result.message()).isEqualTo(testSubject.message());
        assertThat(result.cause().isPresent()).isFalse();
        assertThat(result.enqueuedAt()).isEqualTo(testSubject.enqueuedAt());
        assertThat(result.lastTouched()).isEqualTo(testSubject.lastTouched());
        assertThat(result.diagnostics()).isEqualTo(testSubject.diagnostics());
    }

    @Test
    void invokingWithNullCauseAndOriginalCauseKeepsTheOriginalCause() {
        // Fix the clock to keep time consistent after withCause invocation.
        GenericDeadLetter.clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        Throwable testThrowable = new RuntimeException("just because");
        ThrowableCause expectedCause = new ThrowableCause(testThrowable);

        DeadLetter<EventMessage> testSubject =
                new GenericDeadLetter<>(SEQUENCE_IDENTIFIER, MESSAGE, testThrowable);

        DeadLetter<EventMessage> result = testSubject.withCause(null);

        assertThat(result.message()).isEqualTo(testSubject.message());
        Optional<Cause> resultCause = result.cause();
        assertThat(resultCause.isPresent()).isTrue();
        assertThat(expectedCause).isEqualTo(resultCause.get());
        assertThat(result.enqueuedAt()).isEqualTo(testSubject.enqueuedAt());
        assertThat(result.lastTouched()).isEqualTo(testSubject.lastTouched());
        assertThat(result.diagnostics()).isEqualTo(testSubject.diagnostics());
    }

    @Test
    void invokingWithDiagnosticsReplacesTheOriginalDiagnostics() {
        // Fix the clock to keep time consistent after withDiagnostics invocation.
        Instant expectedTime = Instant.now();
        GenericDeadLetter.clock = Clock.fixed(expectedTime, ZoneId.systemDefault());

        Metadata originalDiagnostics = Metadata.with("old-key", "old-value");
        Metadata expectedDiagnostics = Metadata.with("new-key", "new-value");

        DeadLetter<EventMessage> testSubject = new GenericDeadLetter<>(
                SEQUENCE_IDENTIFIER, MESSAGE, null, expectedTime, expectedTime, originalDiagnostics
        );

        DeadLetter<EventMessage> result = testSubject.withDiagnostics(expectedDiagnostics);

        assertThat(result.message()).isEqualTo(testSubject.message());
        Optional<Cause> resultCause = result.cause();
        assertThat(resultCause.isPresent()).isFalse();
        assertThat(result.enqueuedAt()).isEqualTo(testSubject.enqueuedAt());
        assertThat(result.lastTouched()).isEqualTo(testSubject.lastTouched());
        assertThat(result.diagnostics()).isEqualTo(expectedDiagnostics);
    }

    @Test
    void invokingWithDiagnosticsBuilderAppendsTheOriginalDiagnostics() {
        // Fix the clock to keep time consistent after withDiagnostics invocation.
        Instant expectedTime = Instant.now();
        GenericDeadLetter.clock = Clock.fixed(expectedTime, ZoneId.systemDefault());

        Metadata originalDiagnostics = Metadata.with("old-key", "old-value");
        Metadata expectedDiagnostics = Metadata.with("old-key", "old-value").and("new-key", "new-value");

        DeadLetter<EventMessage> testSubject = new GenericDeadLetter<>(
                SEQUENCE_IDENTIFIER, MESSAGE, null, expectedTime, expectedTime, originalDiagnostics
        );

        DeadLetter<EventMessage> result =
                testSubject.withDiagnostics(original -> original.and("new-key", "new-value"));

        assertThat(result.message()).isEqualTo(testSubject.message());
        Optional<Cause> resultCause = result.cause();
        assertThat(resultCause.isPresent()).isFalse();
        assertThat(result.enqueuedAt()).isEqualTo(testSubject.enqueuedAt());
        assertThat(result.lastTouched()).isEqualTo(testSubject.lastTouched());
        assertThat(result.diagnostics()).isEqualTo(expectedDiagnostics);
    }
}