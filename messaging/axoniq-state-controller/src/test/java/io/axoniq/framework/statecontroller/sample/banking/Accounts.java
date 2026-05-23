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

package io.axoniq.framework.statecontroller.sample.banking;

import io.axoniq.framework.statecontroller.decisions.Decision;
import io.axoniq.framework.statecontroller.decisions.DecisionContext;
import io.axoniq.framework.statecontroller.decisions.StateController;
import io.axoniq.framework.statecontroller.eventstream.EventStream;

/**
 * Reproduces the section-3 target programming model verbatim so the State Controller API can be exercised end
 * to end at compile time. Decisions are not yet wired to a runtime; this class is here to validate the
 * developer-facing surface, not to execute.
 */
public class Accounts {

    @StateController
    public Decision withdraw(Withdraw cmd, DecisionContext ctx) {
        EventStream account = ctx.scope("account", cmd.accountId());

        var closed = account.contains(AccountClosed.class);
        var balance = account.sum(MoneyDeposited.class, MoneyDeposited::amount)
                             .minus(account.sum(MoneyWithdrawn.class, MoneyWithdrawn::amount));

        if (closed.isTrue()) {
            return Decision.reject("account closed");
        }
        if (balance.isLessThan(cmd.amount()).isTrue()) {
            return Decision.reject("insufficient funds");
        }
        return Decision.emit(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
    }
}
