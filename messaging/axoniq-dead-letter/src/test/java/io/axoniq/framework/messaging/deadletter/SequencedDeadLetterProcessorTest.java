/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
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
