/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.deadletter;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

class SequencedDeadLetterProcessorTest {

    @Test
    void processWithContextDelegatesToExistingProcessMethod() {
        // given
        RecordingProcessor testSubject = new RecordingProcessor();

        // when
        boolean result = testSubject.process(letter -> true, new StubProcessingContext()).join();

        // then
        assertThat(result).isTrue();
        assertThat(testSubject.processInvocations).isOne();
    }

    @Test
    void processAnyWithContextDelegatesToExistingProcessAnyMethod() {
        // given
        RecordingProcessor testSubject = new RecordingProcessor();

        // when
        boolean result = testSubject.processAny(new StubProcessingContext()).join();

        // then
        assertThat(result).isFalse();
        assertThat(testSubject.processAnyInvocations).isOne();
        assertThat(testSubject.processInvocations).isZero();
    }

    private static class RecordingProcessor implements SequencedDeadLetterProcessor<Message> {

        private int processInvocations;
        private int processAnyInvocations;

        @Override
        public CompletableFuture<Boolean> process(Predicate<DeadLetter<? extends Message>> sequenceFilter) {
            processInvocations++;
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> processAny() {
            processAnyInvocations++;
            return CompletableFuture.completedFuture(false);
        }
    }
}
