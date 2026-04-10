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

package org.axonframework.messaging.deadletter;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.Metadata;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * An {@link EnqueueDecision} stating a {@link DeadLetter dead letter} should be enqueued.
 *
 * @param <M> An implementation of {@link Message} contained in the {@link DeadLetter dead letter} that's been made a
 *            decision on.
 * @author Steven van Beelen
 * @since 4.6.0
 */
public class ShouldEnqueue<M extends Message> implements EnqueueDecision<M> {

    private final Throwable enqueueCause;
    private final Function<DeadLetter<? extends M>, Metadata> diagnosticsBuilder;

    /**
     * Constructs a default {@link EnqueueDecision}. This decision does not carry any {@link #enqueueCause()} or
     * {@link #withDiagnostics(DeadLetter) diagnostics}.
     */
    public ShouldEnqueue() {
        this(null);
    }

    /**
     * Constructs a default {@link EnqueueDecision} to enqueue using the given {@code enqueueCause} as the
     * {@link #enqueueCause()}.
     *
     * @param enqueueCause The {@link Throwable} that was used to decide to enqueue.
     */
    public ShouldEnqueue(Throwable enqueueCause) {
        this(enqueueCause, d -> Metadata.emptyInstance());
    }

    /**
     * Constructs a default {@link EnqueueDecision} to enqueue using the given {@code enqueueCause} as the
     * {@link #enqueueCause()}.
     *
     * @param enqueueCause       The {@link Throwable} that was used to decide to enqueue.
     * @param diagnosticsBuilder A function constructing diagnostics to append during
     *                           {@link #withDiagnostics(DeadLetter)}.
     */
    public ShouldEnqueue(Throwable enqueueCause, Function<DeadLetter<? extends M>, Metadata> diagnosticsBuilder) {
        this.enqueueCause = enqueueCause;
        this.diagnosticsBuilder = diagnosticsBuilder;
    }

    @Override
    public boolean shouldEnqueue() {
        return true;
    }

    @Override
    public Optional<Throwable> enqueueCause() {
        return Optional.ofNullable(enqueueCause);
    }

    @Override
    public DeadLetter<? extends M> withDiagnostics(DeadLetter<? extends M> letter) {
        return letter.withDiagnostics(diagnosticsBuilder.apply(letter));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShouldEnqueue<?> that = (ShouldEnqueue<?>) o;
        return Objects.equals(enqueueCause, that.enqueueCause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enqueueCause);
    }

    @Override
    public String toString() {
        return "ShouldEnqueue{enqueueCause=" + enqueueCause + '}';
    }
}
