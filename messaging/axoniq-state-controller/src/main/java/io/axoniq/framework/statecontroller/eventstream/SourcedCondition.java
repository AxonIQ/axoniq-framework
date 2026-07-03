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

package io.axoniq.framework.statecontroller.eventstream;

import io.axoniq.framework.statecontroller.conditions.Condition;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract base for {@link Condition} implementations that observe events through their backing
 * {@link SourcedEventStream} as a streaming accumulator rather than over a materialized list.
 * <p>
 * Each subclass implements {@link #accept(EventMessage)} (called once per matching event during the sourced
 * read at seal time) and {@link #finalValue()} (called exactly once after seal to produce the condition's
 * value, which is then stored in this condition's per-instance {@link CompletableFuture}). The class registers
 * itself with the stream on construction, so the contract is: declare every condition before forcing any of
 * them — the framework wires registration eagerly, and a post-seal construction raises
 * {@link LateConditionException} via the stream's {@link SourcedEventStream#register register} guard.
 * <p>
 * {@link #resolveAsync()} (inherited as the primary {@link Condition} operation) triggers
 * {@link SourcedEventStream#seal seal} on first access and then returns this condition's value-future. When the
 * stream's underlying reduce completes, the stream calls {@link #complete()} (or
 * {@link #completeExceptionally(Throwable)} on failure) on every registered accumulator; that single
 * happens-before edge replaces the previous {@code awaitLoaded} blocking handshake and provides safe
 * publication of every accumulator field touched during {@link #accept(EventMessage) accept(...)}.
 * <p>
 * Marked {@link Internal @Internal} because instances are produced by {@link SourcedEventStream}; users obtain
 * conditions through the public {@link io.axoniq.framework.statecontroller.eventstream.EventStream EventStream}
 * surface, never by constructing this directly.
 *
 * @param <T> the value type produced by this condition
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
abstract class SourcedCondition<T> implements Condition<T> {

    protected final SourcedEventStream stream;
    private final CompletableFuture<T> future = new CompletableFuture<>();

    SourcedCondition(SourcedEventStream stream) {
        this.stream = Objects.requireNonNull(stream, "stream must not be null");
        stream.register(this);
    }

    /**
     * Observes one event from the sourced read. Called once per delivered event during seal.
     * <p>
     * Implementations should match by {@link EventMessage#type() event type} (via
     * {@link org.axonframework.messaging.core.QualifiedName QualifiedName}) and avoid invoking
     * {@link EventMessage#payload() payload()} unless the value extraction genuinely requires the deserialized
     * payload (for example, the user supplied a typed mapper). Avoiding {@code payload()} where possible keeps
     * serialized event payloads (e.g. {@code byte[]}) from being deserialized unnecessarily.
     *
     * @param event the next event from the source stream; never {@code null}
     */
    abstract void accept(EventMessage event);

    /**
     * Returns the value this condition has accumulated up to seal time. Called by {@link #complete()} exactly
     * once after the stream's reduce has finished applying every event; its result is stored in the value
     * future returned from {@link #resolveAsync()}.
     *
     * @return the accumulated value
     */
    protected abstract T finalValue();

    @Override
    public final CompletableFuture<T> resolveAsync() {
        stream.triggerResolve();
        return future;
    }

    /**
     * Completes this condition's value-future with {@link #finalValue()}. Called by {@link SourcedEventStream}
     * after the underlying reduce finishes successfully. Any exception raised while computing
     * {@code finalValue()} is routed to {@link #completeExceptionally(Throwable)} so blocking callers see it
     * via {@link Condition#value() value()}.
     */
    final void complete() {
        try {
            future.complete(finalValue());
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
    }

    /**
     * Completes this condition's value-future exceptionally with the given {@code error}. Called by
     * {@link SourcedEventStream} when the underlying reduce fails.
     *
     * @param error the failure to propagate to blocking callers
     */
    final void completeExceptionally(Throwable error) {
        future.completeExceptionally(error);
    }
}
