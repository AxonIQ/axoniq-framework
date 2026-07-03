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

package io.axoniq.framework.statecontroller.runtime;

import io.axoniq.framework.statecontroller.conditions.Condition;
import io.axoniq.framework.statecontroller.History;
import io.axoniq.framework.statecontroller.UncoveredEventException;
import io.axoniq.framework.statecontroller.sample.banking.AccountClosed;
import io.axoniq.framework.statecontroller.sample.banking.MoneyDeposited;
import io.axoniq.framework.statecontroller.sample.banking.MoneyWithdrawn;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine.AppendTransaction;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.axoniq.framework.statecontroller.Outcome.accept;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behavioural tests for the {@link History}/{@link Condition} surface over the lazy loading session: value semantics
 * of every read, the declare-then-resolve batching idiom, the supplementary-read policy for conditions declared after
 * the first resolution, the unbound-root guard, and the DCB coverage guard in {@link OutcomeDispatch}.
 */
class HistoryConditionsTest {

    private InMemoryEventStorageEngine engine;
    private EventStore eventStore;
    private ProcessingContext processingContext;
    private List<EventMessage> capturedAppends;
    private TagResolver tagResolver;

    @BeforeEach
    void setUp() {
        engine = new InMemoryEventStorageEngine();
        eventStore = new StorageEngineBackedEventStore(engine, new SimpleEventBus(), e -> Set.of());
        MessageTypeResolver typeResolver = new ClassBasedMessageTypeResolver();
        tagResolver = event -> switch (event.payload()) {
            case MoneyDeposited deposited -> Set.of(new Tag("account", deposited.accountId()));
            case MoneyWithdrawn withdrawn -> Set.of(new Tag("account", withdrawn.accountId()));
            case AccountClosed closed -> Set.of(new Tag("account", closed.accountId()));
            case null, default -> Set.of();
        };
        processingContext = new StubProcessingContext(new ApplicationContext() {
            @SuppressWarnings("unchecked")
            @Override
            public <C> C component(Class<C> type, @Nullable String name) {
                if (type == EventStore.class) {
                    return (C) eventStore;
                }
                if (type == EventSink.class) {
                    return (C) eventStore;
                }
                if (type == MessageTypeResolver.class) {
                    return (C) typeResolver;
                }
                if (type == TagResolver.class) {
                    return (C) tagResolver;
                }
                throw new ComponentNotFoundException(type, name);
            }
        });
        capturedAppends = new ArrayList<>();
        eventStore.transaction(processingContext).onAppend(capturedAppends::add);
    }

    private History rootHistory() {
        return OutcomeDispatch.sessionFor(processingContext).history();
    }

    private <P> void seed(P payload, Set<Tag> tags) {
        var message = new GenericEventMessage(new MessageType(payload.getClass()), payload);
        TaggedEventMessage<?> tagged = new GenericTaggedEventMessage<>(message, tags);
        @SuppressWarnings("unchecked")
        AppendTransaction<ConsistencyMarker> tx = (AppendTransaction<ConsistencyMarker>)
                engine.appendEvents(AppendCondition.none(), null, List.of(tagged)).join();
        tx.commit().thenCompose(tx::afterCommit).join();
    }

    private void seedAccountActivity() {
        Set<Tag> a1 = Set.of(new Tag("account", "a1"));
        seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1);
        seed(new MoneyDeposited("a1", BigDecimal.valueOf(50)), a1);
        seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(30)), a1);
        seed(new MoneyDeposited("other", BigDecimal.valueOf(999)), Set.of(new Tag("account", "other")));
    }

    @Nested
    class ReadValues {

        @Test
        void conditionsDeclaredBeforeTheFirstResolveAllAnswerFromTheSameRead() {
            // given
            seedAccountActivity();
            History account = rootHistory().of("account", "a1");

            // when — declare every condition first (the idiom), then resolve
            var hasDeposits = account.has(MoneyDeposited.class);
            var neverClosed = account.never(AccountClosed.class);
            var movements = account.count(MoneyDeposited.class, MoneyWithdrawn.class);
            var balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
                                 .minus(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));
            var lastDeposit = account.latest(MoneyDeposited.class);
            var firstDeposit = account.first(MoneyDeposited.class);
            var lastMovement = account.latestOf(MoneyDeposited.class, MoneyWithdrawn.class);

            // then — every condition resolves to the value observed on the a1 slice only
            assertThat(hasDeposits.resolve()).isTrue();
            assertThat(neverClosed.resolve()).isTrue();
            assertThat(movements.resolve()).isEqualTo(3);
            assertThat(balance.resolve()).isEqualByComparingTo("120");
            assertThat(lastDeposit.resolve()).map(MoneyDeposited::amount)
                                             .hasValue(BigDecimal.valueOf(50));
            assertThat(firstDeposit.resolve()).map(MoneyDeposited::amount)
                                              .hasValue(BigDecimal.valueOf(100));
            assertThat(lastMovement.resolve()).isInstanceOf(MoneyWithdrawn.class);
        }

        @Test
        void latestOfResolvesToNullWhenNoneOfTheTypesOccurred() {
            // given — no events at all for this scope
            History account = rootHistory().of("account", "empty");

            // when / then
            assertThat(account.latestOf(MoneyDeposited.class, MoneyWithdrawn.class).resolve()).isNull();
        }

        @Test
        void mapAndCombineComposeWithoutResolving() {
            // given
            seedAccountActivity();
            History account = rootHistory().of("account", "a1");

            // when — a mapped condition and a combined condition, declared before any resolution
            Condition<Boolean> overdrawn = account.total(MoneyDeposited.class, MoneyDeposited::amount)
                                             .minus(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount))
                                             .map(b -> b.signum() < 0);
            Condition<String> summary = account.has(AccountClosed.class)
                                          .combine(account.count(MoneyDeposited.class),
                                                   (closed, deposits) -> closed + "/" + deposits);

            // then
            assertThat(overdrawn.resolve()).isFalse();
            assertThat(summary.resolve()).isEqualTo("false/2");
        }

        @Test
        void factsAcrossTwoScopesResolveFromTheirOwnSlices() {
            // given
            seedAccountActivity();
            History root = rootHistory();
            History a1 = root.of("account", "a1");
            History other = root.of("account", "other");

            // when — declare on both scopes, then resolve (first resolve seals both)
            var a1Deposits = a1.total(MoneyDeposited.class, MoneyDeposited::amount);
            var otherDeposits = other.total(MoneyDeposited.class, MoneyDeposited::amount);

            // then
            assertThat(a1Deposits.resolve()).isEqualByComparingTo("150");
            assertThat(otherDeposits.resolve()).isEqualByComparingTo("999");
        }
    }

    @Nested
    class LateDeclarations {

        @Test
        void aConditionDeclaredAfterTheFirstResolveIsAnsweredByASupplementaryRead() {
            // given
            seedAccountActivity();
            History account = rootHistory().of("account", "a1");

            // when — resolve one condition (seals the scope), then declare a condition touching a new event type
            boolean hasDeposits = account.has(MoneyDeposited.class).resolve();
            var lateBalanceSide = account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount);

            // then — the late condition is answered correctly instead of failing with LateConditionException
            assertThat(hasDeposits).isTrue();
            assertThat(lateBalanceSide.resolve()).isEqualByComparingTo("30");
        }

        @Test
        void aScopeFirstTouchedAfterAnotherScopeResolvedStillLoads() {
            // given
            seedAccountActivity();
            History root = rootHistory();

            // when — resolve on one scope, then touch a scope for the first time
            root.of("account", "a1").has(MoneyDeposited.class).resolve();
            var otherDeposits = root.of("account", "other").total(MoneyDeposited.class, MoneyDeposited::amount);

            // then
            assertThat(otherDeposits.resolve()).isEqualByComparingTo("999");
        }
    }

    @Nested
    class UnboundRoot {

        @Test
        void readingTheUnboundRootFailsFast() {
            History root = rootHistory();

            assertThatIllegalStateException().isThrownBy(() -> root.has(AccountClosed.class))
                                             .withMessageContaining("of(");
            assertThatIllegalStateException().isThrownBy(() -> root.total(MoneyDeposited.class,
                                                                           MoneyDeposited::amount));
            assertThatIllegalStateException().isThrownBy(() -> root.latestOf(MoneyDeposited.class));
        }
    }

    @Nested
    class CoverageGuard {

        @Test
        void anAcceptedEventInsideTheReadBoundaryPasses() {
            // given — a decision that read the account scope including the MoneyWithdrawn type
            seedAccountActivity();
            History account = rootHistory().of("account", "a1");
            account.total(MoneyDeposited.class, MoneyDeposited::amount)
                   .minus(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount))
                   .resolve();

            // when / then — MoneyWithdrawn is tagged account=a1 by the TagResolver, inside the read boundary
            assertThatNoException().isThrownBy(
                    () -> OutcomeDispatch.apply(accept(new MoneyWithdrawn("a1", BigDecimal.ONE)),
                                                processingContext));
            assertThat(capturedAppends).hasSize(1);
        }

        @Test
        void anAcceptedEventOutsideEveryReadBoundaryThrowsUncoveredEventException() {
            // given — the decision only read the account scope
            seedAccountActivity();
            rootHistory().of("account", "a1").has(MoneyDeposited.class).resolve();

            // when / then — UnrelatedNoted resolves to no account tag, so no read boundary covers it
            assertThatThrownBy(() -> OutcomeDispatch.apply(accept(new UnrelatedNoted("elsewhere")),
                                                           processingContext))
                    .isInstanceOf(UncoveredEventException.class)
                    .hasMessageContaining(UnrelatedNoted.class.getName());
            // and — nothing was appended
            assertThat(capturedAppends).isEmpty();
        }

        @Test
        void anUnconditionalAcceptWithoutAnyReadsIsNotGuarded() {
            // given — no History reads at all: a legitimate unconditional creation

            // when / then
            assertThatNoException().isThrownBy(
                    () -> OutcomeDispatch.apply(accept(new UnrelatedNoted("created")), processingContext));
            assertThat(capturedAppends).hasSize(1);
        }
    }

    record UnrelatedNoted(String detail) {

    }
}
