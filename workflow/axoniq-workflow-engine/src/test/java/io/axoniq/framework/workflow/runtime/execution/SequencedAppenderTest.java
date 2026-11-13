/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

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
