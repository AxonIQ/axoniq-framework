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

package io.axoniq.framework.messaging.eventhandling.deadletter.jpa;

import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.Metadata;
import io.axoniq.framework.messaging.deadletter.Cause;
import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.GenericDeadLetter;
import io.axoniq.framework.messaging.deadletter.ThrowableCause;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link DeadLetter} that was saved to the database and reconstructed from it. This object is immutable and should
 * only be changed using the {@link #withCause(Throwable)}, {@link #withDiagnostics(Metadata)} and
 * {@link #markTouched()} functions. These reconstruct a new object with the specified new properties.
 *
 * @param <M> The {@link EventMessage} type of the contained message.
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class JpaDeadLetter<M extends EventMessage> implements DeadLetter<M> {

    private final String id;
    private final Long index;
    private final String sequenceIdentifier;
    private final Instant enqueuedAt;
    private final Instant lastTouched;
    private final Cause cause;
    private final Metadata diagnostics;
    private final M message;
    private final Context context;

    /**
     * Constructs a new {@link JpaDeadLetter} from a {@link DeadLetterEntry}, deserialized diagnostics and a
     * reconstructed message entry containing both the message and its associated context.
     * <p>
     * The context contains restored resources such as tracking token and domain info (aggregate identifier, type,
     * sequence number) that were stored when the dead letter was enqueued.
     *
     * @param entry       The {@link DeadLetterEntry} to construct this letter from.
     * @param diagnostics The deserialized diagnostics {@link Metadata}.
     * @param message     The message that was enqueued.
     * @param context     The context containing restored resources such as tracking token and aggregate data.
     */
    public JpaDeadLetter(DeadLetterEntry entry, Metadata diagnostics, M message, Context context) {
        this.id = entry.getDeadLetterId();
        this.index = entry.getSequenceIndex();
        this.enqueuedAt = entry.getEnqueuedAt();
        this.lastTouched = entry.getLastTouched();
        this.sequenceIdentifier = entry.getSequenceIdentifier();
        if (entry.getCauseType() != null) {
            cause = new ThrowableCause(entry.getCauseType(), entry.getCauseMessage());
        } else {
            cause = null;
        }
        this.diagnostics = diagnostics;
        this.message = message;
        this.context = context;
    }

    /**
     * Constructs a new {@link JpaDeadLetter} with all possible parameters.
     *
     * @param id                 The ID of the {@link DeadLetterEntry}.
     * @param index              The index of the {@link DeadLetterEntry}.
     * @param sequenceIdentifier The sequenceIdentifier of the {@link DeadLetterEntry}.
     * @param enqueuedAt         The time the message was enqueued.
     * @param lastTouched        The time the message was last touched.
     * @param cause              The cause of enqueueing, can be null if it was queued because there was another message
     *                           in the same {@code sequenceIdentifier} queued.
     * @param diagnostics        The diagnostics provided during enqueueing.
     * @param message            The message that was enqueued.
     * @param context            The context containing restored resources such as tracking token and domain info.
     */
    JpaDeadLetter(String id,
                  Long index,
                  String sequenceIdentifier,
                  Instant enqueuedAt,
                  Instant lastTouched,
                  Cause cause,
                  Metadata diagnostics,
                  M message,
                  Context context) {
        this.id = id;
        this.index = index;
        this.sequenceIdentifier = sequenceIdentifier;
        this.enqueuedAt = enqueuedAt;
        this.lastTouched = lastTouched;
        this.cause = cause;
        this.diagnostics = diagnostics;
        this.message = message;
        this.context = context;
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

    /**
     * The ID of the entity in the database.
     *
     * @return The ID of this {@link JpaDeadLetter}.
     */
    public String getId() {
        return id;
    }

    /**
     * The index of the dead letter within its sequence identified by the {@code sequenceIdentifier}. Will ensure the
     * events are kept in the original order.
     *
     * @return The index of this {@link JpaDeadLetter}.
     */
    public Long getIndex() {
        return index;
    }

    /**
     * The sequence identifier of this {@link DeadLetter}. If letters belong to the same sequence, they should be
     * handled sequentially at all times. This is ensured by the {@link #getIndex()} property.
     *
     * @return The sequence identifier of this {@link DeadLetter}.
     */
    public String getSequenceIdentifier() {
        return sequenceIdentifier;
    }

    /**
     * Returns the context associated with this dead letter, containing restored resources such as tracking token and
     * domain info (aggregate identifier, type, sequence number).
     *
     * @return The context with restored resources, or an empty context if no resources were stored.
     */
    @Override
    public Context context() {
        return context != null ? context : Context.empty();
    }

    @Override
    public DeadLetter<M> markTouched() {
        return new JpaDeadLetter<>(id,
                                   index,
                                   sequenceIdentifier,
                                   enqueuedAt,
                                   GenericDeadLetter.clock.instant(),
                                   cause,
                                   diagnostics,
                                   message,
                                   context);
    }

    @Override
    public DeadLetter<M> withCause(Throwable requeueCause) {
        return new JpaDeadLetter<>(id,
                                   index,
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
        return new JpaDeadLetter<>(id,
                                   index,
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
        return "JpaDeadLetter{" +
                "id='" + id + "'" +
                ", index=" + index +
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

        JpaDeadLetter<?> that = (JpaDeadLetter<?>) o;

        if (!id.equals(that.id)) {
            return false;
        }
        if (!index.equals(that.index)) {
            return false;
        }
        if (!sequenceIdentifier.equals(that.sequenceIdentifier)) {
            return false;
        }
        if (!enqueuedAt.equals(that.enqueuedAt)) {
            return false;
        }
        if (!lastTouched.equals(that.lastTouched)) {
            return false;
        }
        if (!Objects.equals(cause, that.cause)) {
            return false;
        }
        if (!Objects.equals(diagnostics, that.diagnostics)) {
            return false;
        }
        if (!Objects.equals(message, that.message)) {
            return false;
        }
        return Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + index.hashCode();
        result = 31 * result + sequenceIdentifier.hashCode();
        result = 31 * result + enqueuedAt.hashCode();
        result = 31 * result + lastTouched.hashCode();
        result = 31 * result + (cause != null ? cause.hashCode() : 0);
        result = 31 * result + (diagnostics != null ? diagnostics.hashCode() : 0);
        result = 31 * result + (message != null ? message.hashCode() : 0);
        result = 31 * result + (context != null ? context.hashCode() : 0);
        return result;
    }
}
