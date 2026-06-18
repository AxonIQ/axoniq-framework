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
import io.axoniq.framework.statecontroller.decisions.DecisionContext;
import io.axoniq.framework.statecontroller.decisions.StateController;
import io.axoniq.framework.statecontroller.history.History;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.test.fixture.AxonTestFixture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

/**
 * Proves the second decision parameter selects the read surface independently of the decision annotation: the
 * same fixture wiring dispatches a {@link io.axoniq.framework.statecontroller.decisions.Decide @Decide} method
 * that injects a {@link DecisionContext} (the lazy {@link AccountsViaContext}), a {@code @Decide} method that
 * injects a {@link History} (the eager {@link Accounts}), and — because the parameter-resolver factories are
 * type-driven, not annotation-driven — a {@link StateController @StateController} method that injects a
 * {@link History} too.
 * <p>
 * All components register through one {@link MessagingConfigurer}, so any cross-talk between the two enhancers or
 * the two parameter-resolver factories would surface here. Each scenario seeds a funded account, dispatches the
 * relevant {@link Withdraw}, and asserts the produced {@link MoneyWithdrawn}, proving the decision reached its
 * accept branch through whichever read surface its signature requested.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class EitherInjectionTest {

    private AxonTestFixture fixture;

    @BeforeEach
    void setUp() {
        var configurer = MessagingConfigurer.create()
                                            .registerCommandHandlingModule(
                                                    CommandHandlingModule.named("ViaContext")
                                                                         .commandHandlers(ch ->
                                                                                                  ch.autodetectedCommandHandlingComponent(
                                                                                                          c -> new AccountsViaContext()
                                                                                                  )))
                                            .registerCommandHandlingModule(
                                                    CommandHandlingModule.named("ViaHistory")
                                                                         .commandHandlers(ch ->
                                                                                                  ch.autodetectedCommandHandlingComponent(
                                                                                                          c -> new HistoryWithdrawals()
                                                                                                  )))
                                            .registerCommandHandlingModule(
                                                    CommandHandlingModule.named("StateControllerViaHistory")
                                                                         .commandHandlers(ch ->
                                                                                                  ch.autodetectedCommandHandlingComponent(
                                                                                                          c -> new StateControllerHistoryWithdrawals()
                                                                                                  )));
        fixture = AxonTestFixture.with(configurer);
    }

    @Nested
    class DecideAnnotation {

        @Test
        void aDecideMethodCanInjectADecisionContext() {
            // given a funded account handled by the lazy @Decide + DecisionContext component
            fixture.given()
                   .events(new MoneyDeposited("ctx", new BigDecimal("100")))
                   // when withdrawing within the balance
                   .when()
                   .command(new Withdraw("ctx", new BigDecimal("40")))
                   // then the lazy DecisionContext decision reached its accept branch
                   .then()
                   .events(new MoneyWithdrawn("ctx", new BigDecimal("40")));
        }

        @Test
        void aDecideMethodCanInjectAHistory() {
            // given a funded account handled by an eager @Decide + History component
            fixture.given()
                   .events(new MoneyDeposited("hist", new BigDecimal("100")))
                   // when withdrawing within the balance
                   .when()
                   .command(new WithdrawViaHistory("hist", new BigDecimal("40")))
                   // then the eager History decision reached its accept branch
                   .then()
                   .events(new MoneyWithdrawn("hist", new BigDecimal("40")));
        }
    }

    @Nested
    class StateControllerAnnotation {

        @Test
        void aStateControllerMethodCanInjectAHistory() {
            // given a funded account handled by a @StateController + History component; the History parameter
            //       resolver is type-driven, so it injects regardless of the decision annotation
            fixture.given()
                   .events(new MoneyDeposited("sc", new BigDecimal("100")))
                   // when withdrawing within the balance
                   .when()
                   .command(new WithdrawViaStateController("sc", new BigDecimal("40")))
                   // then the @StateController + History decision reached its accept branch
                   .then()
                   .events(new MoneyWithdrawn("sc", new BigDecimal("40")));
        }
    }

    // ----------------------------------------------------------------------
    // Commands routed to the History-based components (distinct payload types
    // so each command reaches exactly one of the registered handlers)
    // ----------------------------------------------------------------------

    record WithdrawViaHistory(String accountId, BigDecimal amount) {

    }

    record WithdrawViaStateController(String accountId, BigDecimal amount) {

    }

    // ----------------------------------------------------------------------
    // Decision components proving History injection on both annotations
    // ----------------------------------------------------------------------

    /**
     * Eager {@code @Decide} withdrawal over the {@link History} surface, mirroring {@link Accounts}' rule but
     * routed by a distinct command so it can coexist with the lazy component in one fixture.
     */
    public static class HistoryWithdrawals {

        @Decide
        Decision withdraw(WithdrawViaHistory cmd, History history) {
            History account = history.of("account", cmd.accountId());
            if (account.has(AccountClosed.class)) {
                return Decision.reject("account closed");
            }
            BigDecimal balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
                                        .subtract(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));
            if (balance.compareTo(cmd.amount()) < 0) {
                return Decision.reject("insufficient funds");
            }
            return Decision.accept(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
        }
    }

    /**
     * {@code @StateController} withdrawal that injects a {@link History}. The {@code @StateController} enhancer
     * only requires the method to return a {@link Decision}; the parameter type alone selects the read surface,
     * so the eager {@link History} resolves here just as it does on a {@code @Decide} method.
     */
    public static class StateControllerHistoryWithdrawals {

        @StateController
        public Decision withdraw(WithdrawViaStateController cmd, History history) {
            History account = history.of("account", cmd.accountId());
            if (account.has(AccountClosed.class)) {
                return Decision.reject("account closed");
            }
            BigDecimal balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
                                        .subtract(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));
            if (balance.compareTo(cmd.amount()) < 0) {
                return Decision.reject("insufficient funds");
            }
            return Decision.accept(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
        }
    }
}
