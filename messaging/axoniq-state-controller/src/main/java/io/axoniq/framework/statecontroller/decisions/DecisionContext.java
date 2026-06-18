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
import io.axoniq.framework.statecontroller.history.History;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.time.Instant;
import java.util.Map;

/**
 * The lazy companion handle a decision method receives to declare the slice of event history it cares about.
 * <p>
 * {@code DecisionContext} is the advanced, batched counterpart to the eager {@link History} surface. Inject a
 * {@code DecisionContext} when a decision needs the lazy, deferred-loading conditions produced by
 * {@link #scope(String, Object) scope(...)}; when it spans several scopes that should overlap their reads; when it
 * needs direct access to the in-flight {@link ProcessingContext} (for example to reach a framework component); or
 * when it wants to drop into the simpler, eager {@link History} vocabulary through {@link #history()} or
 * {@link #historyOf(String, Object)}. The vast majority of decisions need none of this and should inject
 * {@link History} directly instead.
 * <p>
 * {@link #scope(String, Object)} declares a single-tag scope (the common case);
 * {@link #scope(Map)} declares a composite scope across multiple tags. Each {@code scope(...)} call returns an
 * {@link EventStream} bound to a loading-context shared with the conditions it produces, so the framework can
 * load every declared question in a single read.
 * <p>
 * {@code DecisionContext} and {@link History} are two views over the same in-flight command: the eager
 * {@link History} reached through {@link #history()} shares the {@link ProcessingContext} and event-store
 * transaction of the lazy scopes, so reads through either surface observe one consistent snapshot and contribute
 * to the same DCB consistency marker. A decision may freely mix the lazy {@code scope(...)} conditions with eager
 * {@link History} reads obtained from this same {@code DecisionContext}.
 * <p>
 * Lazy, batched example:
 * <pre>{@code
 * @StateController
 * public Decision withdraw(Withdraw cmd, DecisionContext ctx) {
 *     EventStream account = ctx.scope("account", cmd.accountId());
 *     var closed  = account.contains(AccountClosed.class);
 *     var balance = account.sum(MoneyDeposited.class, MoneyDeposited::amount)
 *                          .minus(account.sum(MoneyWithdrawn.class, MoneyWithdrawn::amount));
 *
 *     if (closed.isTrue())                           return Decision.reject("account closed");
 *     if (balance.isLessThan(cmd.amount()).isTrue()) return Decision.reject("insufficient funds");
 *     return Decision.emit(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
 * }
 * }</pre>
 * <p>
 * Dropping into the eager {@link History} surface from a {@code DecisionContext}:
 * <pre>{@code
 * @StateController
 * public Decision withdraw(Withdraw cmd, DecisionContext ctx) {
 *     History account = ctx.historyOf("account", cmd.accountId());
 *     if (account.has(AccountClosed.class)) return Decision.reject("account closed");
 *     return Decision.emit(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
 * }
 * }</pre>
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
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

    /**
     * Returns the eager root {@link History} for this decision context, the entry point to the simpler eager read
     * vocabulary.
     * <p>
     * The returned {@code History} is <em>unbound</em>: narrow it to a scope with
     * {@link History#of(String, Object) of(tagKey, tagValue)} (or {@link #historyOf(String, Object)} for the
     * common single-tag case) before reading. It shares the in-flight {@link ProcessingContext} and event-store
     * transaction of this {@code DecisionContext}, so reads through the returned {@code History} and through the
     * lazy {@link #scope(String, Object) scope(...)} conditions observe one consistent snapshot and contribute to
     * the same DCB consistency marker.
     *
     * @return the unbound eager {@link History} for this decision context
     */
    History history();

    /**
     * Returns the eager {@link History} for this decision context already narrowed to the single tag scope
     * {@code tagKey=tagValue}, a shorthand for {@link #history()}{@code .}{@link History#of(String, Object)
     * of(tagKey, tagValue)}.
     *
     * @param tagKey   the tag key identifying the scope (for example {@code "account"})
     * @param tagValue the tag value identifying the concrete entity within the scope; converted with
     *                 {@link Object#toString()}
     * @return an eager {@link History} bound to events tagged {@code tagKey=tagValue}
     */
    default History historyOf(String tagKey, Object tagValue) {
        return history().of(tagKey, tagValue);
    }

    /**
     * Returns the in-flight {@link ProcessingContext} this decision context participates in.
     * <p>
     * Exposed for advanced decisions that must reach a framework component or resource bound to the current
     * command; ordinary decisions never need it. The same {@link ProcessingContext} backs both the lazy
     * {@link #scope(String, Object) scope(...)} conditions and the eager {@link #history()} surface.
     *
     * @return the {@link ProcessingContext} of the in-flight command
     */
    ProcessingContext processingContext();
}
