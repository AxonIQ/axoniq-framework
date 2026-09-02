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

import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class SequencedAppenderTest {

    @Test
    void passesThePositionWrittenByOneAppendToTheNext() {
        var appender = new SequencedAppender();
        var firstPosition = mock(ConsistencyMarker.class);
        var seenPositions = new ArrayList<ConsistencyMarker>();

        appender.appendSequentially(position -> {
            seenPositions.add(position);
            return CompletableFuture.completedFuture(firstPosition);
        }).join();
        appender.appendSequentially(position -> {
            seenPositions.add(position);
            return CompletableFuture.completedFuture(firstPosition);
        }).join();

        assertThat(seenPositions).containsExactly(null, firstPosition);
    }

    @Test
    void failedAppendDoesNotBlockTheAppendBehindIt() {
        var appender = new SequencedAppender();
        var failed = new CompletableFuture<ConsistencyMarker>();
        var secondStarted = new CompletableFuture<Void>();

        var first = appender.appendSequentially(ignored -> failed);
        var second = appender.appendSequentially(ignored -> {
            secondStarted.complete(null);
            return CompletableFuture.completedFuture(mock(ConsistencyMarker.class));
        });
        failed.completeExceptionally(new IllegalStateException("rejected"));

        assertThat(first).isCompletedExceptionally();
        second.join();
        assertThat(secondStarted).isCompleted();
    }
}
