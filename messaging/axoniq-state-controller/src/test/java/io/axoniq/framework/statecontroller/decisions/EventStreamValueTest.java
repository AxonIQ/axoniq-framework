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

import io.axoniq.framework.statecontroller.conditions.BooleanCondition;
import io.axoniq.framework.statecontroller.conditions.Condition;
import io.axoniq.framework.statecontroller.conditions.NumericCondition;
import io.axoniq.framework.statecontroller.conditions.OptionalCondition;
import io.axoniq.framework.statecontroller.eventstream.EventCondition;
import io.axoniq.framework.statecontroller.eventstream.EventStream;
import io.axoniq.framework.statecontroller.sample.banking.AccountClosed;
import io.axoniq.framework.statecontroller.sample.banking.MoneyDeposited;
import io.axoniq.framework.statecontroller.sample.banking.MoneyWithdrawn;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine.AppendTransaction;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 acceptance tests pinning down the value-level behaviour of every helper on
 * {@link EventStream} and the specialized conditions it produces.
 * <p>
 * Where the Phase 2 mechanics test ({@link DecisionContextLoadingTest}) verifies the loading lifecycle
 * (seal-once, late-declaration, single read per scope), this test verifies that conditions return the right
 * values when forced against real loaded events: predicates report correctly, counts and sums aggregate, latest
 * and first selectors pick the right entries, {@code latestMatch} narrows to registered types, {@code fold}
 * composes typed and named reducers, and the specialized condition combinators
 * ({@link BooleanCondition#and}, {@link NumericCondition#minus}, {@link OptionalCondition#orDefault}, etc.) yield
 * the expected booleans, numbers, and optionals.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
class EventStreamValueTest {

    private InMemoryEventStorageEngine engine;
    private EventStore eventStore;
    private ProcessingContext processingContext;

    @BeforeEach
    void setUp() {
        engine = new InMemoryEventStorageEngine();
        eventStore = new StorageEngineBackedEventStore(engine, new SimpleEventBus(), e -> Set.of());
        MessageTypeResolver typeResolver = new ClassBasedMessageTypeResolver();
        processingContext = new StubProcessingContext(new ApplicationContext() {
            @Override
            public <C> C component(Class<C> type, @Nullable String name) {
                if (type == MessageTypeResolver.class) {
                    return type.cast(typeResolver);
                }
                throw new ComponentNotFoundException(type, name);
            }
        });
    }

    private DecisionContextImpl newDecisionContext() {
        return new DecisionContextImpl(eventStore, processingContext, Clock.systemUTC());
    }

    private EventStream a1Scope() {
        return newDecisionContext().scope("account", "a1");
    }

    private <P> void seed(P payload, Set<Tag> tags) {
        seed(new MessageType(payload.getClass()), payload, tags);
    }

    private void seed(MessageType type, Object payload, Set<Tag> tags) {
        var message = new GenericEventMessage(type, payload);
        TaggedEventMessage<?> tagged = new GenericTaggedEventMessage<>(message, tags);
        @SuppressWarnings("unchecked")
        AppendTransaction<ConsistencyMarker> tx = (AppendTransaction<ConsistencyMarker>)
                engine.appendEvents(AppendCondition.none(), null, List.of(tagged)).join();
        tx.commit().thenCompose(tx::afterCommit).join();
    }

    private Set<Tag> a1Tag() {
        return Set.of(new Tag("account", "a1"));
    }

    @Nested
    class Predicates {

        @Test
        void containsReturnsTrueWhenAtLeastOneMatchingEventExists() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());

            // when
            BooleanCondition hasDeposits = a1Scope().contains(MoneyDeposited.class);

            // then
            assertThat(hasDeposits.isTrue()).isTrue();
            assertThat(hasDeposits.isFalse()).isFalse();
        }

        @Test
        void containsReturnsFalseWhenNoMatchingEvents() {
            // given — no events seeded

            // when
            BooleanCondition hasDeposits = a1Scope().contains(MoneyDeposited.class);

            // then
            assertThat(hasDeposits.isTrue()).isFalse();
            assertThat(hasDeposits.isFalse()).isTrue();
        }

        @Test
        void containsAnyOfMatchesWhenAnyOfTheTypesIsPresent() {
            // given — only a deposit exists, but the predicate asks for either type
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());

            // when
            BooleanCondition any = a1Scope().containsAnyOf(MoneyDeposited.class, MoneyWithdrawn.class);

            // then
            assertThat(any.isTrue()).isTrue();
        }

        @Test
        void containsAnyOfReturnsFalseWhenNoneOfTheTypesArePresent() {
            // given
            seed(new AccountClosed("a1"), a1Tag());

            // when — neither type matches the AccountClosed in the stream
            BooleanCondition any = a1Scope().containsAnyOf(MoneyDeposited.class, MoneyWithdrawn.class);

            // then
            assertThat(any.isTrue()).isFalse();
        }

        @Test
        void booleanCombinatorsComposeAsExpected() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new AccountClosed("a1"), a1Tag());

            // when
            EventStream stream = a1Scope();
            BooleanCondition hasDeposit = stream.contains(MoneyDeposited.class);
            BooleanCondition isClosed = stream.contains(AccountClosed.class);
            BooleanCondition hasWithdrawn = stream.contains(MoneyWithdrawn.class);

            // then
            assertThat(hasDeposit.and(isClosed).isTrue()).isTrue();
            assertThat(hasDeposit.and(hasWithdrawn).isTrue()).isFalse();
            assertThat(hasDeposit.or(hasWithdrawn).isTrue()).isTrue();
            assertThat(hasWithdrawn.not().isTrue()).isTrue();
            assertThat(hasDeposit.xor(isClosed).isTrue()).isFalse();
            assertThat(hasDeposit.xor(hasWithdrawn).isTrue()).isTrue();
        }
    }

    @Nested
    class Counting {

        @Test
        void countSumsEventsAcrossAllRequestedTypes() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(50)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(20)), a1Tag());

            // when
            NumericCondition<Long> count = a1Scope().count(MoneyDeposited.class, MoneyWithdrawn.class);

            // then
            assertThat(count.value()).isEqualTo(3L);
        }

        @Test
        void countOverASingleTypeMatchesOnlyEventsOfThatType() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(20)), a1Tag());

            // when
            NumericCondition<Long> deposits = a1Scope().count(MoneyDeposited.class);

            // then
            assertThat(deposits.value()).isEqualTo(1L);
        }

        @Test
        void countReturnsZeroWhenNoEventsMatch() {
            // given — no events seeded

            // when
            NumericCondition<Long> count = a1Scope().count(MoneyDeposited.class);

            // then
            assertThat(count.value()).isEqualTo(0L);
            assertThat(count.isZero().isTrue()).isTrue();
        }
    }

    @Nested
    class Summing {

        @Test
        void sumAggregatesTheTypedProjection() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(50)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(25)), a1Tag());

            // when
            NumericCondition<BigDecimal> total = a1Scope().sum(MoneyDeposited.class, MoneyDeposited::amount);

            // then
            assertThat(total.value()).isEqualByComparingTo("175");
        }

        @Test
        void sumReturnsZeroWhenNoMatchingEvents() {
            // given — no deposits
            seed(new AccountClosed("a1"), a1Tag());

            // when
            NumericCondition<BigDecimal> total = a1Scope().sum(MoneyDeposited.class, MoneyDeposited::amount);

            // then
            assertThat(total.value()).isEqualByComparingTo("0");
        }

        @Test
        void sumMinusSumComputesBalanceAsAFluentChain() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(50)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(30)), a1Tag());

            // when — the plan's banking example: positive balance with sum.minus(sum)
            EventStream account = a1Scope();
            NumericCondition<BigDecimal> balance = account.sum(MoneyDeposited.class, MoneyDeposited::amount)
                                                          .minus(account.sum(MoneyWithdrawn.class,
                                                                             MoneyWithdrawn::amount));

            // then
            assertThat(balance.value()).isEqualByComparingTo("120");
            assertThat(balance.isGreaterThan(BigDecimal.ZERO).isTrue()).isTrue();
            assertThat(balance.isAtLeast(BigDecimal.valueOf(120)).isTrue()).isTrue();
            assertThat(balance.isLessThan(BigDecimal.valueOf(200)).isTrue()).isTrue();
        }

        @Test
        void numericComparisonsYieldExpectedBooleans() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());

            // when
            NumericCondition<BigDecimal> total = a1Scope().sum(MoneyDeposited.class, MoneyDeposited::amount);

            // then
            assertThat(total.isGreaterThan(BigDecimal.valueOf(50)).isTrue()).isTrue();
            assertThat(total.isAtLeast(BigDecimal.valueOf(100)).isTrue()).isTrue();
            assertThat(total.isAtMost(BigDecimal.valueOf(100)).isTrue()).isTrue();
            assertThat(total.isEqualTo(BigDecimal.valueOf(100)).isTrue()).isTrue();
            assertThat(total.isPositive().isTrue()).isTrue();
            assertThat(total.isZero().isTrue()).isFalse();
        }

        @Test
        void numericPlusOnTwoConditionsCombinesNumericDomains() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(40)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(10)), a1Tag());

            // when
            EventStream account = a1Scope();
            NumericCondition<BigDecimal> totalMoved = account.sum(MoneyDeposited.class, MoneyDeposited::amount)
                                                             .plus(account.sum(MoneyWithdrawn.class,
                                                                               MoneyWithdrawn::amount));

            // then
            assertThat(totalMoved.value()).isEqualByComparingTo("50");
        }
    }

    @Nested
    class Selection {

        @Test
        void latestReturnsTheMostRecentMatchingPayload() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(20)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(30)), a1Tag());

            // when
            OptionalCondition<MoneyDeposited> latest = a1Scope().latest(MoneyDeposited.class);

            // then
            assertThat(latest.isPresent().isTrue()).isTrue();
            assertThat(latest.value()).hasValueSatisfying(e ->
                    assertThat(e.amount()).isEqualByComparingTo("30"));
        }

        @Test
        void latestReturnsEmptyWhenNoMatch() {
            // given — only a closure event
            seed(new AccountClosed("a1"), a1Tag());

            // when
            OptionalCondition<MoneyDeposited> latest = a1Scope().latest(MoneyDeposited.class);

            // then
            assertThat(latest.isAbsent().isTrue()).isTrue();
            assertThat(latest.value()).isEmpty();
        }

        @Test
        void firstReturnsTheEarliestMatchingPayload() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(20)), a1Tag());

            // when
            OptionalCondition<MoneyDeposited> first = a1Scope().first(MoneyDeposited.class);

            // then
            assertThat(first.value()).hasValueSatisfying(e ->
                    assertThat(e.amount()).isEqualByComparingTo("10"));
        }

        @Test
        void latestOfYieldsEventConditionOverLastMatch() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(5)), a1Tag());

            // when
            EventCondition latest = a1Scope().latestOf(MoneyDeposited.class, MoneyWithdrawn.class);

            // then
            assertThat(latest.isPresent().isTrue()).isTrue();
            assertThat(latest.isA(MoneyWithdrawn.class).isTrue()).isTrue();
            assertThat(latest.isA(MoneyDeposited.class).isTrue()).isFalse();
            assertThat(latest.isAnyOf(MoneyDeposited.class, AccountClosed.class).isTrue()).isFalse();
        }

        @Test
        void firstOfYieldsEventConditionOverFirstMatch() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(5)), a1Tag());

            // when
            EventCondition first = a1Scope().firstOf(MoneyDeposited.class, MoneyWithdrawn.class);

            // then
            assertThat(first.isA(MoneyDeposited.class).isTrue()).isTrue();
        }

        @Test
        void eventConditionAsNarrowsPayloadByType() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());

            // when
            EventStream stream = a1Scope();
            EventCondition latest = stream.latestOf(MoneyDeposited.class);
            OptionalCondition<MoneyDeposited> narrowed = latest.as(MoneyDeposited.class);

            // then
            assertThat(narrowed.value()).hasValueSatisfying(e ->
                    assertThat(e.amount()).isEqualByComparingTo("10"));
        }

        @Test
        void eventConditionIsNamedComparesQualifiedName() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());

            // when
            EventCondition latest = a1Scope().latestOf(MoneyDeposited.class);

            // then
            assertThat(latest.isNamed(MoneyDeposited.class.getName()).isTrue()).isTrue();
            assertThat(latest.isNamed("does.not.exist").isTrue()).isFalse();
        }
    }

    @Nested
    class Matching {

        @Test
        void latestMatchMapsTheLatestEventOfARegisteredTypeOntoTheSealedResult() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());
            seed(new AccountClosed("a1"), a1Tag());

            // when
            Condition<AccountStatus> status = a1Scope()
                    .latestMatch(AccountStatus.class)
                    .when(AccountClosed.class, e -> new AccountStatus.Closed())
                    .when(MoneyDeposited.class, e -> new AccountStatus.Active())
                    .orDefault(new AccountStatus.Active());

            // then
            assertThat(status.value()).isInstanceOf(AccountStatus.Closed.class);
        }

        @Test
        void latestMatchSelectsLatestAmongRegisteredTypesIgnoringOtherEvents() {
            // given — withdrawal is latest but only deposit/closed types are registered with the matcher
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(5)), a1Tag());

            // when — also register MoneyWithdrawn into the scope load so it shows up in the loaded events,
            // but DO NOT add a when(...) clause for it
            EventStream stream = a1Scope();
            stream.contains(MoneyWithdrawn.class); // forces MoneyWithdrawn into the scope load
            Condition<AccountStatus> status = stream
                    .latestMatch(AccountStatus.class)
                    .when(MoneyDeposited.class, e -> new AccountStatus.Active())
                    .when(AccountClosed.class, e -> new AccountStatus.Closed())
                    .orDefault(new AccountStatus.Unknown());

            // then — even though MoneyWithdrawn is the chronologically latest event, the matcher narrows to
            // events whose type is registered via when(...), so MoneyDeposited wins
            assertThat(status.value()).isInstanceOf(AccountStatus.Active.class);
        }

        @Test
        void latestMatchReturnsDefaultWhenNoRegisteredTypeMatches() {
            // given — only a withdrawal exists; no when clause registers MoneyWithdrawn
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(5)), a1Tag());

            // when
            Condition<AccountStatus> status = a1Scope()
                    .latestMatch(AccountStatus.class)
                    .when(MoneyDeposited.class, e -> new AccountStatus.Active())
                    .when(AccountClosed.class, e -> new AccountStatus.Closed())
                    .orDefault(new AccountStatus.Unknown());

            // then
            assertThat(status.value()).isInstanceOf(AccountStatus.Unknown.class);
        }

        @Test
        void eventConditionMatchingMapsThePreSelectedEvent() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(5)), a1Tag());

            // when — start from latestOf, then map via matching
            Condition<AccountStatus> status = a1Scope()
                    .latestOf(MoneyDeposited.class, MoneyWithdrawn.class)
                    .matching(AccountStatus.class)
                    .when(MoneyDeposited.class, e -> new AccountStatus.Active())
                    .when(MoneyWithdrawn.class, e -> new AccountStatus.Active())
                    .orDefault(new AccountStatus.Unknown());

            // then — the latest event is MoneyWithdrawn, which maps to Active
            assertThat(status.value()).isInstanceOf(AccountStatus.Active.class);
        }
    }

    @Nested
    class Folding {

        @Test
        void foldAppliesTypedReducersInOrderForEachMatchingEvent() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(50)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(30)), a1Tag());

            // when
            BigDecimal balance = a1Scope().fold(BigDecimal.ZERO)
                                          .event(MoneyDeposited.class, (sum, e) -> sum.add(e.amount()))
                                          .event(MoneyWithdrawn.class, (sum, e) -> sum.subtract(e.amount()))
                                          .value();

            // then
            assertThat(balance).isEqualByComparingTo("120");
        }

        @Test
        void foldEqualsSumMinusSumForTheBankingBalance() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(50)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(30)), a1Tag());

            // when — same balance, two different expressions
            EventStream foldStream = a1Scope();
            BigDecimal viaFold = foldStream.fold(BigDecimal.ZERO)
                                           .event(MoneyDeposited.class, (s, e) -> s.add(e.amount()))
                                           .event(MoneyWithdrawn.class, (s, e) -> s.subtract(e.amount()))
                                           .value();

            EventStream sumStream = a1Scope();
            BigDecimal viaSum = sumStream.sum(MoneyDeposited.class, MoneyDeposited::amount)
                                         .minus(sumStream.sum(MoneyWithdrawn.class, MoneyWithdrawn::amount))
                                         .value();

            // then
            assertThat(viaFold).isEqualByComparingTo(viaSum);
            assertThat(viaFold).isEqualByComparingTo("120");
        }

        @Test
        void foldByQualifiedNameMatchesEvents() {
            // given — publish a MoneyDeposited under a custom QualifiedName (the cross-language path)
            QualifiedName custom = new QualifiedName("banking", "MoneyDeposited");
            seed(new MessageType(custom),
                 new MoneyDeposited("a1", BigDecimal.valueOf(75)),
                 a1Tag());

            // when
            BigDecimal total = a1Scope().fold(BigDecimal.ZERO)
                                        .event(custom, (sum, em) -> sum.add(((MoneyDeposited) em.payload()).amount()))
                                        .value();

            // then
            assertThat(total).isEqualByComparingTo("75");
        }

        @Test
        void foldOnAnEmptyStreamReturnsTheInitialValue() {
            // given — no events

            // when
            BigDecimal total = a1Scope().fold(BigDecimal.valueOf(42))
                                        .event(MoneyDeposited.class, (s, e) -> s.add(e.amount()))
                                        .value();

            // then
            assertThat(total).isEqualByComparingTo("42");
        }

        @Test
        void foldFirstMatchingReducerWinsPerEvent() {
            // given — register two reducers; the first matching one for each event applies
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());

            // when — both clauses match MoneyDeposited; the first wins (returns 1)
            Integer counts = a1Scope().fold(0)
                                      .event(MoneyDeposited.class, (n, e) -> n + 1)
                                      .event(MoneyDeposited.class, (n, e) -> n + 100)
                                      .value();

            // then
            assertThat(counts).isEqualTo(1);
        }
    }

    @Nested
    class Combinators {

        @Test
        void conditionMapTransformsTheValueLazily() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());

            // when
            Condition<String> labelled = a1Scope().sum(MoneyDeposited.class, MoneyDeposited::amount)
                                                  .map(b -> "balance=" + b);

            // then
            assertThat(labelled.value()).isEqualTo("balance=100");
        }

        @Test
        void conditionZipCombinesTwoConditions() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(50)), a1Tag());

            // when
            EventStream account = a1Scope();
            NumericCondition<Long> count = account.count(MoneyDeposited.class);
            NumericCondition<BigDecimal> total = account.sum(MoneyDeposited.class, MoneyDeposited::amount);
            Condition<String> summary = count.zip(total, (c, t) -> c + " deposits totaling " + t);

            // then
            assertThat(summary.value()).isEqualTo("2 deposits totaling 150");
        }

        @Test
        void optionalConditionOrDefaultUnwrapsToTheContainedOrFallback() {
            // given — no deposits; default applies
            seed(new AccountClosed("a1"), a1Tag());

            // when
            Condition<MoneyDeposited> dep = a1Scope().latest(MoneyDeposited.class)
                                                     .orDefault(new MoneyDeposited("a1", BigDecimal.ZERO));

            // then
            assertThat(dep.value().amount()).isEqualByComparingTo("0");
        }

        @Test
        void optionalConditionMapPresentThreadsTransformationWithoutForcing() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(40)), a1Tag());

            // when
            OptionalCondition<BigDecimal> latestAmount = a1Scope().latest(MoneyDeposited.class)
                                                                  .mapPresent(MoneyDeposited::amount);

            // then
            assertThat(latestAmount.value()).hasValueSatisfying(a ->
                    assertThat(a).isEqualByComparingTo("40"));
        }
    }

    @Nested
    class TypeResolverWiring {

        @Test
        void registeredEventsUseTheResolverFromTheProcessingContext() {
            // given — a resolver that maps MoneyDeposited to a custom QualifiedName, and an event published
            //         under that same custom name (mimicking the @Event(name=...) path)
            QualifiedName customName = new QualifiedName("banking", "Deposited");
            MessageTypeResolver custom = clazz -> clazz == MoneyDeposited.class
                    ? java.util.Optional.of(new MessageType(customName))
                    : java.util.Optional.of(new MessageType(clazz));
            processingContext = new StubProcessingContext(new ApplicationContext() {
                @Override
                public <C> C component(Class<C> type, @Nullable String name) {
                    if (type == MessageTypeResolver.class) {
                        return type.cast(custom);
                    }
                    throw new ComponentNotFoundException(type, name);
                }
            });
            seed(new MessageType(customName),
                 new MoneyDeposited("a1", BigDecimal.valueOf(80)),
                 a1Tag());

            // when — the SourcedEventStream should resolve MoneyDeposited.class to the custom QualifiedName via
            //        ProcessingContext.component(MessageTypeResolver.class), narrowing the SourcingCondition
            //        to the custom name. If the resolver were ignored, the stored event (published under the
            //        custom name) would not be matched.
            NumericCondition<BigDecimal> total =
                    new DecisionContextImpl(eventStore, processingContext, Clock.systemUTC())
                            .scope("account", "a1")
                            .sum(MoneyDeposited.class, MoneyDeposited::amount);

            // then
            assertThat(total.value()).isEqualByComparingTo("80");
        }

        @Test
        void matchingNeverDeserializesPayloads_predicatesUseMessageTypeOnly() {
            // given — an event whose stored payload class disagrees with the registered class. This simulates
            // the serialized-payload case (e.g. a raw byte[] arriving from the wire) where calling
            // event.payload() would deserialize, while event.type().qualifiedName() is metadata that's always
            // available without touching the payload. The framework's matching must use only the metadata.
            QualifiedName depositName = new QualifiedName(MoneyDeposited.class);
            // The "payload" we store is a String — deliberately NOT a MoneyDeposited. If any matching path
            // calls type::isInstance on this payload, the test will see a false negative.
            seed(new MessageType(depositName), "raw-serialized-bytes-stand-in", a1Tag());

            // when — predicates over MoneyDeposited
            EventStream account = a1Scope();
            BooleanCondition hasDeposit = account.contains(MoneyDeposited.class);
            NumericCondition<Long> deposits = account.count(MoneyDeposited.class);
            EventCondition latestDeposit = account.latestOf(MoneyDeposited.class);

            // then — all predicates correctly identify the event by its MessageType, with no payload touched
            assertThat(hasDeposit.isTrue()).isTrue();
            assertThat(deposits.value()).isEqualTo(1L);
            assertThat(latestDeposit.isA(MoneyDeposited.class).isTrue()).isTrue();
            assertThat(latestDeposit.isNamed(MoneyDeposited.class.getName()).isTrue()).isTrue();
        }
    }

    @Nested
    class MultiScopeValues {

        @Test
        void conditionsOnDistinctScopesSeeOnlyTheirOwnEvents() {
            // given — events under two different account tags
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), Set.of(new Tag("account", "a1")));
            seed(new MoneyDeposited("a2", BigDecimal.valueOf(200)), Set.of(new Tag("account", "a2")));

            // when
            DecisionContextImpl dc = newDecisionContext();
            NumericCondition<BigDecimal> a1Balance = dc.scope("account", "a1")
                                                       .sum(MoneyDeposited.class, MoneyDeposited::amount);
            NumericCondition<BigDecimal> a2Balance = dc.scope("account", "a2")
                                                       .sum(MoneyDeposited.class, MoneyDeposited::amount);

            // then
            assertThat(a1Balance.value()).isEqualByComparingTo("100");
            assertThat(a2Balance.value()).isEqualByComparingTo("200");
        }

        @Test
        void compositeTagScopeRequiresAllTagsToBePresent() {
            // given — event under (account, country); single-tag and composite-tag scopes
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)),
                 Set.of(new Tag("account", "a1"), new Tag("country", "NL")));

            // when
            DecisionContextImpl dc = newDecisionContext();
            BigDecimal viaComposite = dc.scope(java.util.Map.of("account", "a1", "country", "NL"))
                                        .sum(MoneyDeposited.class, MoneyDeposited::amount)
                                        .value();

            // then
            assertThat(viaComposite).isEqualByComparingTo("100");

            // and when — composite scope with a non-matching country yields no events
            DecisionContextImpl dc2 = newDecisionContext();
            BigDecimal missingCountry = dc2.scope(java.util.Map.of("account", "a1", "country", "US"))
                                           .sum(MoneyDeposited.class, MoneyDeposited::amount)
                                           .value();

            // then
            assertThat(missingCountry).isEqualByComparingTo("0");
        }
    }

    // ----------------------------------------------------------------------
    // Test fixtures
    // ----------------------------------------------------------------------

    sealed interface AccountStatus {

        record Active() implements AccountStatus {

        }

        record Closed() implements AccountStatus {

        }

        record Unknown() implements AccountStatus {

        }
    }
}
