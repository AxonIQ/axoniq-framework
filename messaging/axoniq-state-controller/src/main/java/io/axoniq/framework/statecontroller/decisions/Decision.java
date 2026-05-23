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

import java.util.List;
import java.util.Objects;

/**
 * The result of a {@link StateController @StateController} decision: either an {@link Accept} carrying the events
 * to append, or a {@link Reject} carrying a reason (and optionally an audit trail).
 * <p>
 * Decisions are values. They never mutate state directly; the framework reads the decision and translates an
 * {@link Accept} into a conditional append against the event store using the DCB consistency boundary recorded
 * during the decision's condition evaluation, and a {@link Reject} into a denial of the command (with audit
 * events, if any, appended outside the consistency boundary).
 * <p>
 * Use the static factories — {@link #emit(Object...)} for an accept, {@link #reject(String)} for a rejection —
 * rather than instantiating the records directly; the factories are the documented surface and read as the
 * intent does in a decision body.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public sealed interface Decision {

    /**
     * Indicates the command was accepted and produces the events to append.
     *
     * @param events the events to append to the event store under the recorded DCB consistency boundary
     */
    record Accept(List<Object> events) implements Decision {

        /**
         * Compact constructor producing a defensive immutable copy of {@code events}.
         */
        public Accept {
            Objects.requireNonNull(events, "events must not be null");
            events = List.copyOf(events);
        }
    }

    /**
     * Indicates the command was rejected. Carries a human-readable reason and an optional list of audit events
     * that are appended outside the consistency boundary so a rejection can still leave a trace.
     *
     * @param reason      a short reason for the rejection, surfaced to the caller
     * @param auditEvents events to append regardless of the rejection, outside the consistency boundary
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
         * Returns a copy of this rejection that records the given audit events.
         *
         * @param events the events to append as audit trail alongside the rejection
         * @return a {@code Reject} carrying the same reason and the given audit events
         */
        public Reject recording(Object... events) {
            return new Reject(reason, List.of(events));
        }
    }

    /**
     * Returns an {@link Accept} decision carrying the given events, to be appended under the recorded DCB
     * consistency boundary.
     *
     * @param events the events to append on success
     * @return an {@code Accept} decision over those events
     */
    static Decision emit(Object... events) {
        return new Accept(List.of(events));
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
