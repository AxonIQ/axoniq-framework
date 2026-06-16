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

import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.test.fixture.AxonTestFixture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

/**
 * Deeper lifecycle tests for the {@code transfer} decision, written against Axon Framework's
 * {@link AxonTestFixture} and mirroring the {@code AccountsTest} setup (one annotated {@link Accounts} component
 * over an in-memory event store, prior events seeded through the configured {@link EventSink} where
 * {@link org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver} auto-tags them off the
 * {@link org.axonframework.eventsourcing.annotation.EventTag @EventTag} on each event's {@code accountId} field).
 * <p>
 * These scenarios extend, and do not duplicate, the debit+credit / closed-target / insufficient-funds cases
 * already pinned by {@code AccountsTest}. They cover the balance-boundary arithmetic
 * ({@code balance.compareTo(amount) == 0} succeeds, a balance computed across both deposits and withdrawals), the
 * round-trip tagging of the credit leg (the credited target can fund a follow-up withdrawal), and two behaviours
 * deliberately left as-is by the decision: a self-transfer emits both legs for the same account (net zero) and a
 * transfer <em>from</em> a closed source still succeeds because the decision only guards a closed <em>target</em>.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class AccountsTransferTest {

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
    class BalanceBoundary {

        @Test
        void aTransferOfTheExactBalanceSucceeds() {
            // given a source funded with exactly the transfer amount (compareTo == 0 path)
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("60")))
                   // when transferring the whole balance to an open target
                   .when()
                   .command(new TransferMoney("a1", "a2", new BigDecimal("60")))
                   // then the transfer is accepted: balance == amount is sufficient, not under-funded
                   .then()
                   .events(new MoneyWithdrawn("a1", new BigDecimal("60")),
                           new MoneyDeposited("a2", new BigDecimal("60")));
        }

        @Test
        void aBalanceComputedAcrossDepositsAndWithdrawalsCoversAnExactTransfer() {
            // given deposits 100 + 50 and a withdrawal 30, leaving a balance of 120
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("100")),
                           new MoneyDeposited("a1", new BigDecimal("50")),
                           new MoneyWithdrawn("a1", new BigDecimal("30")))
                   // when transferring exactly the computed balance
                   .when()
                   .command(new TransferMoney("a1", "a2", new BigDecimal("120")))
                   // then the transfer succeeds against the net balance
                   .then()
                   .events(new MoneyWithdrawn("a1", new BigDecimal("120")),
                           new MoneyDeposited("a2", new BigDecimal("120")));
        }

        @Test
        void aTransferOneOverTheComputedBalanceIsRejected() {
            // given the same net balance of 120 (deposits 100 + 50, withdrawal 30)
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("100")),
                           new MoneyDeposited("a1", new BigDecimal("50")),
                           new MoneyWithdrawn("a1", new BigDecimal("30")))
                   // when transferring one more than the balance
                   .when()
                   .command(new TransferMoney("a1", "a2", new BigDecimal("121")))
                   // then the command is rejected on insufficient funds and nothing is appended
                   .then()
                   .exceptionSatisfies(t -> assertThat(t.getMessage()).contains("insufficient funds"))
                   .noEvents();
        }
    }

    @Nested
    class CreditLegRoundTrip {

        @Test
        void theCreditedTargetCanFundASubsequentWithdrawalUpToTheCreditedAmount() {
            // given a funded source, then a transfer that credits the target (first command, committed in its
            // own unit of work during the given phase so its events are visible to the when-phase command)
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("100")))
                   .command(new TransferMoney("a1", "a2", new BigDecimal("60")))
                   // when withdrawing from the now-credited target account
                   .when()
                   .command(new Withdraw("a2", new BigDecimal("40")))
                   // then the withdrawal against the transferred balance is recorded — proving the credit leg's
                   // MoneyDeposited was tagged to the target and is readable in the a2 scope
                   .then()
                   .events(new MoneyWithdrawn("a2", new BigDecimal("40")));
        }

        @Test
        void theCreditedTargetCannotBeOverdrawnBeyondTheCreditedAmount() {
            // given a transfer of 60 into a2, seeded directly as its result events (auto-tagged off @EventTag)
            fixture.given()
                   .events(new MoneyWithdrawn("a1", new BigDecimal("60")),
                           new MoneyDeposited("a2", new BigDecimal("60")))
                   // when withdrawing more than the credited amount from the target
                   .when()
                   .command(new Withdraw("a2", new BigDecimal("61")))
                   // then the target's balance is exactly the credited 60, so the over-withdrawal is rejected
                   .then()
                   .exceptionSatisfies(t -> assertThat(t.getMessage()).contains("insufficient funds"))
                   .noEvents();
        }
    }

    @Nested
    class EdgeBehaviours {

        @Test
        void aSelfTransferEmitsBothLegsForTheSameAccountNettingToZero() {
            // given a funded account
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("100")))
                   // when transferring from the account to itself (fromId == toId)
                   .when()
                   .command(new TransferMoney("a1", "a1", new BigDecimal("40")))
                   // then current behaviour: both a debit and a credit are emitted for a1, netting to zero —
                   // the decision does not special-case fromId == toId
                   .then()
                   .events(new MoneyWithdrawn("a1", new BigDecimal("40")),
                           new MoneyDeposited("a1", new BigDecimal("40")));
        }

        @Test
        void aTransferFromAClosedSourceCurrentlySucceeds() {
            // given a funded source that is also closed, and an open target
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("100")),
                           new AccountClosed("a1"))
                   // when transferring from the closed (but funded) source
                   .when()
                   .command(new TransferMoney("a1", "a2", new BigDecimal("40")))
                   // then current behaviour: the transfer SUCCEEDS. The decision only guards a closed TARGET
                   // (to.has(AccountClosed)), never a closed SOURCE — an intentional asymmetry pinned here.
                   // This documents existing behaviour; the decision is NOT changed.
                   .then()
                   .events(new MoneyWithdrawn("a1", new BigDecimal("40")),
                           new MoneyDeposited("a2", new BigDecimal("40")));
        }
    }

    // Imported as a static method so the inline lambda assertions read cleanly.
    private static org.assertj.core.api.AbstractStringAssert<?> assertThat(String s) {
        return org.assertj.core.api.Assertions.assertThat(s);
    }
}
