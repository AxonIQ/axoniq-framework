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
package io.axoniq.workflow.runtime.execution;

import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class SequencedAppendConditionTest {

    @Test
    void passesThePositionWrittenByOneAppendToTheNext() {
        var condition = new SequencedAppendCondition();
        var firstPosition = mock(ConsistencyMarker.class);
        var seenPositions = new ArrayList<ConsistencyMarker>();

        condition.appendSequentially(position -> {
            seenPositions.add(position);
            return CompletableFuture.completedFuture(firstPosition);
        }).join();
        condition.appendSequentially(position -> {
            seenPositions.add(position);
            return CompletableFuture.completedFuture(firstPosition);
        }).join();

        assertThat(seenPositions).containsExactly(null, firstPosition);
    }

    @Test
    void failedAppendDoesNotBlockTheAppendBehindIt() {
        var condition = new SequencedAppendCondition();
        var failed = new CompletableFuture<ConsistencyMarker>();
        var secondStarted = new CompletableFuture<Void>();

        var first = condition.appendSequentially(ignored -> failed);
        var second = condition.appendSequentially(ignored -> {
            secondStarted.complete(null);
            return CompletableFuture.completedFuture(mock(ConsistencyMarker.class));
        });
        failed.completeExceptionally(new IllegalStateException("rejected"));

        assertThat(first).isCompletedExceptionally();
        second.join();
        assertThat(secondStarted).isCompleted();
    }

    @Test
    void rejectionIsRecognizedThroughTheCauseChain() {
        var rejected = AppendEventsTransactionRejectedException.conflictingEventsDetected(ConsistencyMarker.ORIGIN);

        assertThat(AppendFailureClassifier.isRejected(rejected)).isTrue();
        assertThat(AppendFailureClassifier.isRejected(new CompletionException(rejected))).isTrue();
        assertThat(AppendFailureClassifier.isRejected(new CompletionException(new RuntimeException(rejected))))
                .isTrue();
        assertThat(AppendFailureClassifier.isRejected(new IllegalStateException("boom"))).isFalse();
        assertThat(AppendFailureClassifier.isRejected(null)).isFalse();
    }
}
