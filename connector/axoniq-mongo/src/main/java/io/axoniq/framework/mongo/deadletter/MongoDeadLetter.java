/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import io.axoniq.framework.messaging.deadletter.Cause;
import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.GenericDeadLetter;
import io.axoniq.framework.messaging.deadletter.ThrowableCause;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link DeadLetter} that was saved to MongoDB and reconstructed from it. This object is immutable and should only be
 * changed using the {@link #withCause(Throwable)}, {@link #withDiagnostics(Metadata)} and {@link #markTouched()}
 * functions. These reconstruct a new object with the specified new properties.
 *
 * @param <M> the {@link EventMessage} type of the contained message
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class MongoDeadLetter<M extends EventMessage> implements DeadLetter<M> {

    private final long index;
    private final String sequenceIdentifier;
    private final Instant enqueuedAt;
    private final Instant lastTouched;
    private final @Nullable Cause cause;
    private final Metadata diagnostics;
    private final M message;
    private final Context context;

    /**
     * Constructs a new {@code MongoDeadLetter} from a {@link DeadLetterEntry}, deserialized diagnostics, the
     * reconstructed message, and the context restored alongside it.
     * <p>
     * The context contains restored resources such as the tracking token and aggregate information that were stored
     * when the dead letter was enqueued.
     *
     * @param entry       the {@link DeadLetterEntry} to construct this letter from
     * @param diagnostics the deserialized diagnostics {@link Metadata}
     * @param message     the reconstructed {@link EventMessage}
     * @param context     the {@link Context} containing the restored resources
     */
    public MongoDeadLetter(DeadLetterEntry entry, Metadata diagnostics, M message, Context context) {
        this.index = entry.getIndex();
        this.enqueuedAt = entry.getEnqueuedAt();
        this.lastTouched = entry.getLastTouched();
        this.sequenceIdentifier = entry.getSequenceIdentifier();
        String causeType = entry.getCauseType();
        this.cause = causeType != null ? new ThrowableCause(causeType, entry.getCauseMessage()) : null;
        this.diagnostics = diagnostics;
        this.message = message;
        this.context = context;
    }

    /**
     * Constructs a new {@code MongoDeadLetter} with all possible parameters.
     *
     * @param index              the index of the {@link DeadLetterEntry}
     * @param sequenceIdentifier the sequence identifier of the {@link DeadLetterEntry}
     * @param enqueuedAt         the time the message was enqueued
     * @param lastTouched        the time the message was last touched
     * @param cause              the cause of enqueueing, can be {@code null} if it was queued because there was another
     *                           message in the same {@code sequenceIdentifier} queued
     * @param diagnostics        the diagnostics provided during enqueueing
     * @param message            the message that was enqueued
     * @param context            the {@link Context} containing the tracking token and aggregate information
     */
    @SuppressWarnings("squid:S107")
    MongoDeadLetter(long index,
                    String sequenceIdentifier,
                    Instant enqueuedAt,
                    Instant lastTouched,
                    @Nullable Cause cause,
                    Metadata diagnostics,
                    M message,
                    Context context) {
        this.index = index;
        this.sequenceIdentifier = sequenceIdentifier;
        this.enqueuedAt = enqueuedAt;
        this.lastTouched = lastTouched;
        this.cause = cause;
        this.diagnostics = diagnostics;
        this.message = message;
        this.context = context;
    }

    /**
     * The index of the dead letter within its sequence identified by the {@code sequenceIdentifier}. Will ensure the
     * events are kept in the original order.
     *
     * @return the index of this {@code MongoDeadLetter}
     */
    public long index() {
        return index;
    }

    /**
     * The sequence identifier for the {@link #message()} to be dead lettered.
     *
     * @return the {@link String} sequence identifier for the {@link #message()} to be dead lettered
     */
    public String sequenceIdentifier() {
        return sequenceIdentifier;
    }

    @Override
    public M message() {
        return message;
    }

    @Override
    public Optional<Cause> cause() {
        return Optional.ofNullable(cause);
    }

    @Override
    public Instant enqueuedAt() {
        return enqueuedAt;
    }

    @Override
    public Instant lastTouched() {
        return lastTouched;
    }

    @Override
    public Metadata diagnostics() {
        return diagnostics;
    }

    @Override
    public Context context() {
        return context;
    }

    @Override
    public DeadLetter<M> markTouched() {
        return new MongoDeadLetter<>(index,
                                     sequenceIdentifier,
                                     enqueuedAt,
                                     GenericDeadLetter.clock.instant(),
                                     cause,
                                     diagnostics,
                                     message,
                                     context);
    }

    @Override
    public DeadLetter<M> withCause(@Nullable Throwable requeueCause) {
        return new MongoDeadLetter<>(index,
                                     sequenceIdentifier,
                                     enqueuedAt,
                                     GenericDeadLetter.clock.instant(),
                                     requeueCause != null ? ThrowableCause.asCause(requeueCause) : cause,
                                     diagnostics,
                                     message,
                                     context);
    }

    @Override
    public DeadLetter<M> withDiagnostics(Metadata diagnostics) {
        return new MongoDeadLetter<>(index,
                                     sequenceIdentifier,
                                     enqueuedAt,
                                     GenericDeadLetter.clock.instant(),
                                     cause,
                                     diagnostics,
                                     message,
                                     context);
    }

    @Override
    public String toString() {
        return "MongoDeadLetter{" +
                "index=" + index +
                ", sequenceIdentifier='" + sequenceIdentifier + "'" +
                ", enqueuedAt=" + enqueuedAt +
                ", lastTouched=" + lastTouched +
                ", cause=" + cause +
                ", diagnostics=" + diagnostics +
                ", message=" + message +
                ", context=" + context +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoDeadLetter<?> that = (MongoDeadLetter<?>) o;
        return index == that.index
                && sequenceIdentifier.equals(that.sequenceIdentifier)
                && enqueuedAt.equals(that.enqueuedAt)
                && lastTouched.equals(that.lastTouched)
                && Objects.equals(cause, that.cause)
                && Objects.equals(diagnostics, that.diagnostics)
                && Objects.equals(message, that.message)
                && Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, sequenceIdentifier, enqueuedAt, lastTouched, cause, diagnostics, message, context);
    }
}
