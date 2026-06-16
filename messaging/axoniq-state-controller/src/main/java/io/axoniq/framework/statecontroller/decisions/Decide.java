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

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a decision handler in the business-first decision API.
 * <p>
 * The first parameter is the command payload — its runtime type determines which command is routed to this
 * handler. The second parameter is a {@link History}, the event history injected into the decision and narrowed
 * to the scope(s) it cares about with {@link History#of(String, Object)}. The return type is a {@link Decision},
 * produced via {@link Decision#accept(Object...)} / {@link Decision#reject(String)}.
 * <p>
 * The annotation is meta-annotated with {@link CommandHandler @CommandHandler}, so AF5's existing handler
 * discovery picks up {@code @Decide} methods automatically as command handlers. A
 * {@link org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition HandlerEnhancerDefinition}
 * registered by this module ({@link DecideHandlerEnhancer}) wraps each annotated method so that:
 * <ul>
 *     <li>a returned {@link Decision.Accept} appends its events through the in-context
 *         {@link org.axonframework.messaging.eventhandling.gateway.EventAppender EventAppender} and surfaces
 *         {@link Decision.Accept#result()} as the handler's return value (or {@code null} if none was set);</li>
 *     <li>a returned {@link Decision.Reject} appends its audit events through the same appender and throws a
 *         {@link org.axonframework.messaging.commandhandling.CommandExecutionException CommandExecutionException}
 *         carrying the rejection reason as its message and the audit events as its
 *         {@link org.axonframework.messaging.commandhandling.CommandExecutionException#getDetails() details}.</li>
 * </ul>
 * The enclosing class itself carries no annotation; decisions live on an ordinary application bean and the
 * framework discovers decision handlers by scanning registered classes (or explicit instances) for methods
 * annotated with {@code @Decide}.
 * <p>
 * A given method carries either {@code @Decide} <em>or</em> {@link StateController @StateController}, never both:
 * the two annotations express the same decision-handler role over two different read surfaces (the business-first
 * {@link History} versus the lower-level {@link DecisionContext}), so combining them on one method is a
 * programming error.
 * <p>
 * Example:
 * <pre>{@code
 * public class Accounts {
 *
 *     @Decide
 *     public Decision withdraw(Withdraw cmd, History history) {
 *         History account = history.of("account", cmd.accountId());
 *         if (account.has(AccountClosed.class)) {
 *             return reject("account closed");
 *         }
 *         BigDecimal balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
 *                                     .subtract(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));
 *         if (balance.compareTo(cmd.amount()) < 0) {
 *             return reject("insufficient funds");
 *         }
 *         return accept(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
 *     }
 * }
 * }</pre>
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@CommandHandler
public @interface Decide {
}
