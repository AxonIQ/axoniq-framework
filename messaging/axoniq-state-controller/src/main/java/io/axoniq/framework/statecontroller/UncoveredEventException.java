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

package io.axoniq.framework.statecontroller;

import org.axonframework.common.AxonNonTransientException;
import org.axonframework.messaging.core.QualifiedName;

/**
 * Thrown when an {@link Outcome.Accept accepted} outcome emits an event that no read scope covers, breaking the
 * Dynamic Consistency Boundary (DCB) optimistic lock the decision relied on.
 * <p>
 * The State Controller's DCB guarantee rests on a simple contract: the consistency boundary an accepted append is
 * checked against is exactly the union of the boundaries the decision read. A decision reads with
 * {@code history.of("account", id)} (a tag-scoped read) and is expected to emit events tagged so they fall inside
 * that same scope. The read tag and the appended-event tag are wired independently — the read derives its
 * {@link org.axonframework.messaging.eventstreaming.EventCriteria EventCriteria} from {@code history.of(...)}, the
 * append derives its tags from the configured
 * {@link org.axonframework.eventsourcing.eventstore.TagResolver TagResolver} (typically
 * {@link org.axonframework.eventsourcing.annotation.EventTag @EventTag} on the event) — so they can silently
 * drift apart. When they do, the optimistic lock guards the wrong (or empty) surface and two concurrent commands
 * can both commit, a silent double-spend.
 * <p>
 * To close that gap, every accepted event must be covered by at least one boundary the decision actually read.
 * This exception names the offending event and advises adding an
 * {@link org.axonframework.eventsourcing.annotation.EventTag @EventTag} whose tags match a scope the decision read
 * via {@code history.of(...)} (or restricting that scope to the event's type). It does not fire when the decision
 * read no scope at all — an unconditional append (for example a creation) is a legitimate choice — nor for the
 * audit events appended alongside an {@link Outcome.Reject rejection}.
 * <p>
 * Extends {@link AxonNonTransientException} because an uncovered event is a deterministic wiring error: retrying
 * the same command unchanged cannot resolve it; the event or the read scope must be corrected.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
public class UncoveredEventException extends AxonNonTransientException {

    /**
     * Constructs an {@code UncoveredEventException} for the accepted event of the given {@code eventType} resolved
     * to {@code eventName}, none of whose tags fall inside any consistency boundary the decision read.
     *
     * @param eventType the runtime class of the accepted event that no read scope covers
     * @param eventName the {@link QualifiedName} the event resolved to, as the DCB read compared against
     */
    public UncoveredEventException(Class<?> eventType, QualifiedName eventName) {
        super(buildMessage(eventType, eventName));
    }

    private static String buildMessage(Class<?> eventType, QualifiedName eventName) {
        return "Accepted event [" + eventType.getName() + "] (resolved to [" + eventName
                + "]) is not covered by any consistency boundary this decision read. "
                + "Its tags fall outside every scope read via history.of(...), so the DCB optimistic lock would "
                + "guard the wrong surface and concurrent commands could both commit. "
                + "Tag the event to match a read scope (for example add @EventTag whose key/value matches the "
                + "history.of(...) the decision reads), or narrow that read scope to include this event's type.";
    }
}
