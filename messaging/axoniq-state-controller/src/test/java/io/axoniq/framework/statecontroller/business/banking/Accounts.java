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

import io.axoniq.framework.statecontroller.decisions.Decide;
import io.axoniq.framework.statecontroller.decisions.Decision;
import io.axoniq.framework.statecontroller.history.History;

import java.math.BigDecimal;

import static io.axoniq.framework.statecontroller.decisions.Decision.accept;
import static io.axoniq.framework.statecontroller.decisions.Decision.reject;

/**
 * Banking decisions in the business-first {@code @Decide} / {@link History} API, transcribed from the ADR's
 * "Single scope" and "Multiple scopes" worked examples. Both read with the bare-payload vocabulary
 * ({@link History#has(Class) has}, {@link History#total(Class, java.util.function.Function) total}) over scopes
 * narrowed with {@code history.of("account", id)}.
 * <p>
 * {@link #withdraw(Withdraw, History) withdraw} narrows a single account scope: it rejects a closed account,
 * computes the balance as deposits minus withdrawals, rejects on insufficient funds, and otherwise emits a
 * {@link MoneyWithdrawn}. {@link #transfer(TransferMoney, History) transfer} narrows two account scopes in one
 * decision — the DCB cross-entity payoff — rejecting a closed target or an under-funded source and otherwise
 * emitting a {@link MoneyWithdrawn} on the source followed by a {@link MoneyDeposited} on the target, both
 * inside one consistency boundary spanning the two accounts.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
public class Accounts {

    @Decide
    Decision withdraw(Withdraw cmd, History history) {
        History account = history.of("account", cmd.accountId());
        if (account.has(AccountClosed.class)) return reject("account closed");

        BigDecimal balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
                             .subtract(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));
        if (balance.compareTo(cmd.amount()) < 0) return reject("insufficient funds");

        return accept(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
    }

    @Decide
    Decision transfer(TransferMoney cmd, History history) {
        History from = history.of("account", cmd.fromId());
        History to   = history.of("account", cmd.toId());

        if (to.has(AccountClosed.class))         return reject("target account closed");

        BigDecimal balance = from.total(MoneyDeposited.class, MoneyDeposited::amount)
                             .subtract(from.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));
        if (balance.compareTo(cmd.amount()) < 0) return reject("insufficient funds");

        return accept(new MoneyWithdrawn(cmd.fromId(), cmd.amount()),
                      new MoneyDeposited(cmd.toId(),   cmd.amount()));
    }
}
