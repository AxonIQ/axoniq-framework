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

import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.test.fixture.AxonTestFixture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A deliberately "full-power" test for the {@code closeAccount} decision, exercising several of the new
 * {@code @Decide} / {@link io.axoniq.framework.statecontroller.history.History History} API features in one
 * decision:
 * <ul>
 *     <li>a <em>type-precise</em> scope built with {@code history.of("account", id, types...)} — the boundary
 *     covers exactly the money-movement and closure events, not every event sharing the {@code account} tag;</li>
 *     <li>a balance folded with {@code total(...)} and a lifetime transaction {@code count(...)};</li>
 *     <li>an audit event {@code recording(...)} on rejection, surfaced to the caller as the
 *     {@link CommandExecutionException#getDetails() exception details};</li>
 *     <li>a {@link ClosingStatement} {@code returning(...)} value, asserted via the fixture's
 *     {@code resultMessagePayloadSatisfies(...)}.</li>
 * </ul>
 * Wired exactly like {@code AccountsTest}: one annotated {@link Accounts} component over an in-memory event store,
 * with {@link org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver} auto-tagging seeded and
 * emitted events off each event's {@link org.axonframework.eventsourcing.annotation.EventTag @EventTag}.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class AccountClosureTest {

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
    class Accepts {

        @Test
        void closingAZeroBalanceAccountReturnsAStatementCountingEveryTransaction() {
            // given deposits and withdrawals that net to zero, across three money movements
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("100")),
                           new MoneyWithdrawn("a1", new BigDecimal("30")),
                           new MoneyWithdrawn("a1", new BigDecimal("70")))
                   // when closing the account
                   .when()
                   .command(new CloseAccount("a1"))
                   // then it closes, and the returned statement reports all three transactions
                   .then()
                   .resultMessagePayloadSatisfies(ClosingStatement.class, statement -> {
                       assertThat(statement.accountId()).isEqualTo("a1");
                       assertThat(statement.transactions()).isEqualTo(3L);
                   })
                   .events(new AccountClosed("a1"));
        }
    }

    @Nested
    class Rejects {

        @Test
        void closingAnAccountWithANonZeroBalanceIsRejectedAndRecordsAnAuditEvent() {
            // given an account still holding 100
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("100")))
                   // when attempting to close it
                   .when()
                   .command(new CloseAccount("a1"))
                   // then it is rejected, and the denial is recorded as an audit event carrying the balance,
                   // surfaced through the CommandExecutionException details
                   .then()
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("balance must be zero");
                       List<Object> details = ((CommandExecutionException) t).<List<Object>>getDetails()
                                                                              .orElse(List.of());
                       assertThat(details).singleElement()
                                          .isInstanceOfSatisfying(AccountClosureRejected.class, rejected -> {
                                              assertThat(rejected.accountId()).isEqualTo("a1");
                                              assertThat(rejected.balance()).isEqualByComparingTo("100");
                                          });
                   });
        }

        @Test
        void closingAnAlreadyClosedAccountIsRejected() {
            // given a balanced account that has already been closed
            fixture.given()
                   .events(new MoneyDeposited("a1", new BigDecimal("50")),
                           new MoneyWithdrawn("a1", new BigDecimal("50")),
                           new AccountClosed("a1"))
                   // when attempting to close it again
                   .when()
                   .command(new CloseAccount("a1"))
                   // then the command is rejected and nothing further is appended
                   .then()
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("already closed");
                   })
                   .noEvents();
        }
    }
}
