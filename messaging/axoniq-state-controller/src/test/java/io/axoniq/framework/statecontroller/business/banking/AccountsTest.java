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

import org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.test.fixture.AxonTestFixture;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;

/**
 * Lifecycle test for the banking sample's business-first decisions, written against Axon Framework's
 * {@link AxonTestFixture}. Prior events are seeded through the configured {@link EventSink} (where
 * {@link AnnotationBasedTagResolver} auto-tags them off the
 * {@link org.axonframework.eventsourcing.annotation.EventTag @EventTag} on each event's {@code accountId}
 * field), a command is dispatched, and the appended events / thrown exception are asserted.
 * <p>
 * The fixture configures one annotated {@link Accounts} component on top of an in-memory event store, and lets
 * the {@code @Decide} handler enhancer + {@code History} parameter-resolver factory (both
 * ServiceLoader-discovered) wire the decision dispatch path automatically. The {@code transfer} scenarios
 * exercise the cross-entity DCB payoff: a single decision narrows two {@code account} scopes and appends a debit
 * on the source plus a credit on the target.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class AccountsTest {

    private AxonTestFixture fixture;

    @BeforeEach
    void setUp() {
        var configurer = MessagingConfigurer.create().registerCommandHandlingModule(
                CommandHandlingModule.named("Accounts")
                                     .commandHandlers(ch ->
                                                              ch.autodetectedCommandHandlingComponent(
                                                                      c -> new Accounts()
                                                              )));
        fixture = AxonTestFixture.with(configurer);
    }

    @Nested
    class WithdrawFlow {

        @Test
        void aDepositCoversASubsequentWithdrawal() {
            // given a funded account
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("100")))
                   // when withdrawing within the balance
                   .when()
                   .command(new Withdraw("a1", new BigDecimal("40")))
                   // then the withdrawal is recorded
                   .then()
                   .events(new MoneyWithdrawn("a1", new BigDecimal("40")));
        }

        @Test
        void withdrawingMoreThanTheBalanceIsRejected() {
            // given an account with only 30 deposited
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("30")))
                   // when withdrawing more than the balance
                   .when()
                   .command(new Withdraw("a1", new BigDecimal("50")))
                   // then the command is rejected and nothing is appended
                   .then()
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("insufficient funds");
                   })
                   .noEvents();
        }

        @Test
        void withdrawingFromAClosedAccountIsRejected() {
            // given a funded but closed account
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("100")),
                           new AccountClosed("a1"))
                   // when attempting a withdrawal
                   .when()
                   .command(new Withdraw("a1", new BigDecimal("10")))
                   // then the command is rejected and nothing is appended
                   .then()
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("account closed");
                   })
                   .noEvents();
        }
    }

    @Nested
    class TransferFlow {

        @Test
        void aTransferDebitsTheSourceAndCreditsTheTarget() {
            // given a funded source account and an open target account
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("100")))
                   // when transferring within the source balance
                   .when()
                   .command(new TransferMoney("a1", "a2", new BigDecimal("60")))
                   // then the source is debited and the target credited, in that order, within one boundary
                   .then()
                   .events(new MoneyWithdrawn("a1", new BigDecimal("60")),
                           new MoneyDeposited("a2", new BigDecimal("60")));
        }

        @Test
        void aTransferToAClosedTargetIsRejected() {
            // given a funded source and a closed target
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("100")),
                           new AccountClosed("a2"))
                   // when transferring to the closed target
                   .when()
                   .command(new TransferMoney("a1", "a2", new BigDecimal("40")))
                   // then the command is rejected and nothing is appended
                   .then()
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("target account closed");
                   })
                   .noEvents();
        }

        @Test
        void aTransferExceedingTheSourceBalanceIsRejected() {
            // given a source with only 20 deposited and an open target
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("20")))
                   // when transferring more than the source balance
                   .when()
                   .command(new TransferMoney("a1", "a2", new BigDecimal("50")))
                   // then the command is rejected and nothing is appended
                   .then()
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("insufficient funds");
                   })
                   .noEvents();
        }
    }

    // Imported as a static method so the inline lambda assertions read cleanly.
    private static org.assertj.core.api.AbstractThrowableAssert<?, ? extends Throwable> assertThat(Throwable t) {
        return org.assertj.core.api.Assertions.assertThat(t);
    }

    private static org.assertj.core.api.AbstractStringAssert<?> assertThat(String s) {
        return org.assertj.core.api.Assertions.assertThat(s);
    }
}
