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

import io.axoniq.framework.statecontroller.eventstream.EventStream;

import java.time.Instant;
import java.util.Map;

/**
 * The lower-level handle a state-controlled command handler can receive to declare the slice of event history it
 * cares about through raw, composable conditions.
 * <p>
 * This is the advanced counterpart to the {@link io.axoniq.framework.statecontroller.History History}
 * surface: inject a {@code DecisionContext} when a decision needs the full
 * {@link io.axoniq.framework.statecontroller.conditions.Condition Condition} vocabulary (custom folds, match
 * builders) that the simpler History surface deliberately omits. Both surfaces share the same per-command loading
 * session, so mixing them still batches reads and records one DCB consistency boundary.
 * <p>
 * {@link #scope(String, Object)} declares a single-tag scope (the common case);
 * {@link #scope(Map)} declares a composite scope across multiple tags. Each {@code scope(...)} call returns an
 * {@link EventStream} bound to a loading-context shared with the conditions it produces, so the framework can
 * load every declared question in a single read.
 * <p>
 * Example:
 * <pre>{@code
 * @CommandHandler
 * public Outcome withdraw(Withdraw cmd, DecisionContext ctx) {
 *     EventStream account = ctx.scope("account", cmd.accountId());
 *     var closed  = account.contains(AccountClosed.class);
 *     var balance = account.sum(MoneyDeposited.class, MoneyDeposited::amount)
 *                          .minus(account.sum(MoneyWithdrawn.class, MoneyWithdrawn::amount));
 *
 *     if (closed.isTrue())                           return Outcome.reject("account closed");
 *     if (balance.isLessThan(cmd.amount()).isTrue()) return Outcome.reject("insufficient funds");
 *     return Outcome.accept(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
 * }
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public interface DecisionContext {

    /**
     * Returns an {@link EventStream} containing the events tagged with the given key/value pair.
     *
     * @param tagKey   the tag key identifying the entity or slice this decision concerns
     * @param tagValue the tag value, typically an entity identifier; the framework converts it to a string
     * @return the lazily-loaded event stream for the tagged slice
     */
    EventStream scope(String tagKey, Object tagValue);

    /**
     * Returns an {@link EventStream} containing the events tagged with all of the given key/value pairs.
     * <p>
     * Use this overload when a decision spans multiple identifiers (for example, the source and destination
     * accounts in a transfer). The framework will resolve the union of relevant events for the composite scope.
     *
     * @param tags the tag key/value pairs defining the scope; the framework converts values to strings
     * @return the lazily-loaded event stream for the composite tagged slice
     */
    EventStream scope(Map<String, ?> tags);

    /**
     * Returns the current time according to the framework-supplied clock.
     * <p>
     * Decision bodies should read the current time through this method rather than calling
     * {@link Instant#now()} directly, so they stay deterministic under test.
     *
     * @return the current {@link Instant} as observed through the framework clock
     */
    Instant time();
}
