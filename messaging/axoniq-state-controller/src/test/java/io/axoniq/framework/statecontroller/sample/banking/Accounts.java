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

import io.axoniq.framework.statecontroller.History;
import io.axoniq.framework.statecontroller.Outcome;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;

import static io.axoniq.framework.statecontroller.Outcome.accept;
import static io.axoniq.framework.statecontroller.Outcome.reject;

/**
 * Banking decisions in the target programming model: plain {@link CommandHandler @CommandHandler} methods whose
 * {@link History} parameter and {@link Outcome} return type opt them into the State Controller — no dedicated
 * annotation.
 * <p>
 * Both handlers follow the idiomatic shape: declare every condition first, then resolve. The first
 * {@code resolve()} loads all declared conditions — across all scopes — in a single event-store read whose criteria
 * (the tags narrowed via {@code of(...)} and the event types the conditions name) double as the decision's DCB
 * consistency boundary.
 */
public class Accounts {

    @CommandHandler
    public Outcome withdraw(Withdraw cmd, History history) {
        History account = history.of("account", cmd.accountId());

        var closed = account.has(AccountClosed.class);
        var balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
                             .minus(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));

        if (closed.resolve()) {
            return reject("account closed");
        }
        if (balance.resolve().compareTo(cmd.amount()) < 0) {
            return reject("insufficient funds");
        }
        return accept(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
    }

    @CommandHandler
    public Outcome transfer(TransferMoney cmd, History history) {
        History from = history.of("account", cmd.fromAccountId());
        History to = history.of("account", cmd.toAccountId());

        // Declaring conditions on both scopes before the first resolve() lets the framework load them together.
        var fromClosed = from.has(AccountClosed.class);
        var toClosed = to.has(AccountClosed.class);
        var balance = from.total(MoneyDeposited.class, MoneyDeposited::amount)
                          .minus(from.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));

        if (fromClosed.resolve()) {
            return reject("source account closed");
        }
        if (toClosed.resolve()) {
            return reject("target account closed");
        }
        if (balance.resolve().compareTo(cmd.amount()) < 0) {
            return reject("insufficient funds");
        }
        return accept(new MoneyWithdrawn(cmd.fromAccountId(), cmd.amount()),
                      new MoneyDeposited(cmd.toAccountId(), cmd.amount()));
    }
}
