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

package io.axoniq.framework.statecontroller.business.banking;

import io.axoniq.framework.statecontroller.conditions.BooleanCondition;
import io.axoniq.framework.statecontroller.conditions.NumericCondition;
import io.axoniq.framework.statecontroller.decisions.Decide;
import io.axoniq.framework.statecontroller.decisions.Decision;
import io.axoniq.framework.statecontroller.decisions.DecisionContext;
import io.axoniq.framework.statecontroller.eventstream.EventStream;

import java.math.BigDecimal;

import static io.axoniq.framework.statecontroller.decisions.Decision.accept;
import static io.axoniq.framework.statecontroller.decisions.Decision.reject;

/**
 * The lazy, batched sibling of the eager {@link Accounts}: the same banking rules and rejection messages,
 * written against the {@link DecisionContext} surface instead of {@link io.axoniq.framework.statecontroller.history.History
 * History}. Where {@link Accounts} reads eagerly with {@code history.of(...).total(...)}, this class narrows a
 * scope with {@link DecisionContext#scope(String, Object) ctx.scope("account", id)}, declares the questions it
 * cares about as lazy {@link io.axoniq.framework.statecontroller.conditions.Condition Condition}s, and forces
 * them with {@link io.axoniq.framework.statecontroller.conditions.Condition#resolve() resolve()} — so all
 * questions on one scope share a single coordinated read.
 * <p>
 * Both methods carry {@link Decide @Decide}, demonstrating that the business-first annotation accepts the lazy
 * {@link DecisionContext} just as it accepts the eager {@code History}: the second parameter's type, not the
 * annotation, selects the read surface.
 * <p>
 * {@link #withdraw(Withdraw, DecisionContext) withdraw} narrows a single account scope, computing the balance as
 * a {@link NumericCondition#minus(io.axoniq.framework.statecontroller.conditions.Condition) deposits minus
 * withdrawals} difference and forcing the closed-flag and the balance comparison from the same read.
 * {@link #transfer(TransferMoney, DecisionContext) transfer} narrows two account scopes in one decision — the DCB
 * cross-entity payoff — rejecting a closed source or target, or an under-funded source, and otherwise emitting a
 * {@link MoneyWithdrawn} on the source followed by a {@link MoneyDeposited} on the target.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
public class AccountsViaContext {

    @Decide
    Decision withdraw(Withdraw cmd, DecisionContext ctx) {
        EventStream account = ctx.scope("account", cmd.accountId());

        BooleanCondition closed = account.contains(AccountClosed.class);
        NumericCondition<BigDecimal> balance =
                account.sum(MoneyDeposited.class, MoneyDeposited::amount)
                       .minus(account.sum(MoneyWithdrawn.class, MoneyWithdrawn::amount));

        if (closed.resolve()) {
            return reject("account closed");
        }
        if (balance.resolve().compareTo(cmd.amount()) < 0) {
            return reject("insufficient funds");
        }
        return accept(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
    }

    @Decide
    Decision transfer(TransferMoney cmd, DecisionContext ctx) {
        EventStream from = ctx.scope("account", cmd.fromId());
        EventStream to = ctx.scope("account", cmd.toId());

        BooleanCondition fromClosed = from.contains(AccountClosed.class);
        BooleanCondition toClosed = to.contains(AccountClosed.class);
        // The credit is appended to the target, so its type must fall inside a scope this decision read: read the
        // target's money movements so MoneyDeposited lands inside the target's consistency boundary (covering the
        // appended credit and guarding it against concurrent money movements on the target).
        to.containsAnyOf(MoneyDeposited.class, MoneyWithdrawn.class);
        NumericCondition<BigDecimal> fromBalance =
                from.sum(MoneyDeposited.class, MoneyDeposited::amount)
                    .minus(from.sum(MoneyWithdrawn.class, MoneyWithdrawn::amount));

        if (fromClosed.resolve()) {
            return reject("source account closed");
        }
        if (toClosed.resolve()) {
            return reject("target account closed");
        }
        if (fromBalance.resolve().compareTo(cmd.amount()) < 0) {
            return reject("insufficient funds");
        }
        return accept(new MoneyWithdrawn(cmd.fromId(), cmd.amount()),
                      new MoneyDeposited(cmd.toId(), cmd.amount()));
    }
}
