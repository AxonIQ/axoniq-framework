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

package io.axoniq.framework.statecontroller.decisions;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * The result of a {@link StateController @StateController} decision: either an {@link Accept} carrying the events
 * to append (optionally with a result value to surface to the command caller), or a {@link Reject} carrying a
 * reason (and optionally an audit trail).
 * <p>
 * Decisions are values. They never mutate state directly; the framework reads the decision and translates an
 * {@link Accept} into a conditional append against the event store using the DCB consistency boundary recorded
 * during the decision's condition evaluation, and a {@link Reject} into a denial of the command (with audit
 * events, if any, appended through the same {@code EventStoreTransaction}).
 * <p>
 * Use the static factories — {@link #emit(Object...)} for an accept, {@link #reject(String)} for a rejection —
 * rather than instantiating the records directly; the factories are the documented surface and read as the
 * intent does in a decision body. When the command caller needs to receive a value (for example a generated
 * identifier), chain {@link Accept#returning(Object)} on the accept to attach it; if no result is supplied, the
 * caller observes {@code null}.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public sealed interface Decision {

    /**
     * Indicates the command was accepted and produces the events to append.
     *
     * @param events the events to append to the event store under the recorded DCB consistency boundary
     * @param result optional value to surface to the command caller as the handler's return value; {@code null}
     *               when the caller does not need a result
     */
    record Accept(List<Object> events, @Nullable Object result) implements Decision {

        /**
         * Compact constructor producing a defensive immutable copy of {@code events}.
         */
        public Accept {
            Objects.requireNonNull(events, "events must not be null");
            events = List.copyOf(events);
        }

        /**
         * Returns a copy of this acceptance that carries the given {@code result} back to the command caller.
         * <p>
         * Common use: a command handler that creates a new aggregate may want to surface the generated identifier
         * as the command's return value, while still appending the underlying creation event(s).
         *
         * @param result the value to return from the command dispatch; may be {@code null} to explicitly clear a
         *               previously set result
         * @return an {@code Accept} carrying the same events and the given {@code result}
         */
        public Accept returning(@Nullable Object result) {
            return new Accept(events, result);
        }
    }

    /**
     * Indicates the command was rejected. Carries a human-readable reason and an optional list of audit events
     * appended through the same {@code EventStoreTransaction} so a rejection can still leave a trace.
     *
     * @param reason      a short reason for the rejection, surfaced to the caller via
     *                    {@link org.axonframework.messaging.commandhandling.CommandExecutionException#getMessage()
     *                    CommandExecutionException.getMessage()}
     * @param auditEvents events to append regardless of the rejection; surfaced to the caller via
     *                    {@link org.axonframework.messaging.commandhandling.CommandExecutionException#getDetails()
     *                    CommandExecutionException.getDetails()}
     */
    record Reject(String reason, List<Object> auditEvents) implements Decision {

        /**
         * Compact constructor validating non-null arguments and producing a defensive immutable copy of
         * {@code auditEvents}.
         */
        public Reject {
            Objects.requireNonNull(reason, "reason must not be null");
            Objects.requireNonNull(auditEvents, "auditEvents must not be null");
            auditEvents = List.copyOf(auditEvents);
        }

        /**
         * Returns a copy of this rejection whose audit events are replaced with the given {@code events}. Any
         * previously-recorded audit events on this {@code Reject} are discarded; recording is therefore
         * idempotent rather than additive.
         *
         * @param events the events to append as audit trail alongside the rejection; the array and its
         *               elements must not be {@code null}
         * @return a {@code Reject} carrying the same reason and the given audit events
         */
        public Reject recording(Object... events) {
            Objects.requireNonNull(events, "events must not be null");
            return new Reject(reason, List.of(events));
        }
    }

    /**
     * Returns an {@link Accept} decision carrying the given events, to be appended under the recorded DCB
     * consistency boundary. Chain {@link Accept#returning(Object)} to attach a result for the command caller.
     *
     * @param events the events to append on success; the array and its elements must not be {@code null}
     * @return an {@code Accept} decision over those events with no result
     */
    static Accept emit(Object... events) {
        Objects.requireNonNull(events, "events must not be null");
        return new Accept(List.of(events), null);
    }

    /**
     * Returns a {@link Reject} decision with the given reason and no audit events. Chain
     * {@link Reject#recording(Object...)} on the result to attach an audit trail.
     *
     * @param reason a short reason for the rejection, surfaced to the caller
     * @return a {@code Reject} decision carrying {@code reason}
     */
    static Reject reject(String reason) {
        return new Reject(reason, List.of());
    }
}
