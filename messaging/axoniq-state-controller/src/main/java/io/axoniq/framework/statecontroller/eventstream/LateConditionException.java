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

/**
 * Thrown when a decision body attempts to declare a new condition (or register a new event type) on an
 * {@link io.axoniq.framework.statecontroller.eventstream.EventStream EventStream} whose scope has already been
 * sealed by an earlier {@link io.axoniq.framework.statecontroller.conditions.Condition#value() value()} call on
 * one of its conditions.
 * <p>
 * Each scope opened by {@link DecisionContext#scope DecisionContext.scope(...)} loads every condition declared
 * on it in a single coordinated read against the event store. Forcing any condition on that scope seals the
 * read at the position observed; declaring a new condition (for example through
 * {@code stream.contains(NewType.class)}) on the same scope after that point would silently fall outside the
 * consistency boundary captured for the scope, so the framework refuses to do it. Sealing is per-scope —
 * forcing a condition on one scope does not prevent another scope from being opened or extended.
 * <p>
 * The exception's own stack trace identifies the offending invocation; no extra call-site capture is needed.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public final class LateConditionException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new {@code LateConditionException} describing the attempted declaration.
     *
     * @param description short description of what was being declared (for example, an event type or a tag set)
     */
    public LateConditionException(String description) {
        super("Cannot declare " + description + " after the scope has been sealed. "
                      + "This stream reference was sealed by an earlier resolution "
                      + "(every Condition.resolve() / resolveAsync() call seals the declared scopes); "
                      + "look the scope up again to declare more conditions via a supplementary read.");
    }
}
