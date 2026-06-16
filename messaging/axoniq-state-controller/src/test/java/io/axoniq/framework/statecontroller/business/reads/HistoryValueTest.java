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

package io.axoniq.framework.statecontroller.business.reads;

import io.axoniq.framework.statecontroller.history.History;
import io.axoniq.framework.statecontroller.history.HistoryFactory;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine.AppendTransaction;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Value-level acceptance tests pinning down what every {@link History} read returns when forced against real,
 * seeded events.
 * <p>
 * Events are appended through an in-memory {@link StorageEngineBackedEventStore} over an
 * {@link InMemoryEventStorageEngine} and read back through a {@link History} minted by
 * {@link HistoryFactory#rootHistoryFor(ProcessingContext)} on a {@link StubProcessingContext} that exposes the
 * {@link EventStore} and a {@link MessageTypeResolver}. The tests cover the membership reads
 * ({@code has} / {@code never} / {@code lastWas} / {@code count}), the value-returning selectors
 * ({@code latest} / {@code latestOf} / {@code first} / {@code total} / {@code entry}), the advanced
 * {@code matching(EventCriteria)} narrowing, and the guards that reject reads on the unbound root and a second
 * {@code of(...)} on an already-narrowed history.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class HistoryValueTest {

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
                if (type == EventStore.class) {
                    return type.cast(eventStore);
                }
                throw new ComponentNotFoundException(type, name);
            }
        });
    }

    private History rootHistory() {
        return HistoryFactory.rootHistoryFor(processingContext);
    }

    private History a1Scope() {
        return rootHistory().of("account", "a1");
    }

    private <P> void seed(P payload, Set<Tag> tags) {
        var message = new GenericEventMessage(new MessageType(payload.getClass()), payload);
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
    class Has {

        @Test
        void hasReturnsTrueWhenAtLeastOneMatchingEventExists() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());

            // when / then
            assertThat(a1Scope().has(MoneyDeposited.class)).isTrue();
        }

        @Test
        void hasReturnsFalseWhenNoMatchingEvents() {
            // given — only an unrelated event in the scope
            seed(new AccountClosed("a1"), a1Tag());

            // when / then
            assertThat(a1Scope().has(MoneyDeposited.class)).isFalse();
        }

        @Test
        void neverIsTheInverseOfHas() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());

            // when
            History account = a1Scope();

            // then
            assertThat(account.never(MoneyDeposited.class)).isFalse();
            assertThat(account.never(MoneyWithdrawn.class)).isTrue();
        }
    }

    @Nested
    class LastWas {

        @Test
        void lastWasReturnsTrueWhenNewestEventInScopeIsOfType() {
            // given — withdrawal is the newest event across the whole scope
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(20)), a1Tag());

            // when / then
            assertThat(a1Scope().lastWas(MoneyWithdrawn.class)).isTrue();
        }

        @Test
        void lastWasReturnsFalseWhenNewestEventIsAnotherType() {
            // given — the newest event is a withdrawal, not a deposit
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(20)), a1Tag());

            // when / then — boundary is the tag, so the latest deposit does not make lastWas(deposit) true
            assertThat(a1Scope().lastWas(MoneyDeposited.class)).isFalse();
        }

        @Test
        void lastWasReturnsFalseWhenScopeIsEmpty() {
            // given — no events seeded

            // when / then
            assertThat(a1Scope().lastWas(MoneyDeposited.class)).isFalse();
        }
    }

    @Nested
    class Latest {

        @Test
        void latestReturnsTheMostRecentMatchingPayload() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(20)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(30)), a1Tag());

            // when / then
            assertThat(a1Scope().latest(MoneyDeposited.class))
                    .hasValueSatisfying(e -> assertThat(e.amount()).isEqualByComparingTo("30"));
        }

        @Test
        void latestSkipsInterveningEventsOfOtherTypes() {
            // given — a younger withdrawal sits between the deposits and the read
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(20)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(5)), a1Tag());

            // when / then — latest deposit is unaffected by the trailing withdrawal
            assertThat(a1Scope().latest(MoneyDeposited.class))
                    .hasValueSatisfying(e -> assertThat(e.amount()).isEqualByComparingTo("20"));
        }

        @Test
        void latestReturnsEmptyWhenNoMatch() {
            // given — only an unrelated event
            seed(new AccountClosed("a1"), a1Tag());

            // when / then
            assertThat(a1Scope().latest(MoneyDeposited.class)).isEmpty();
        }
    }

    @Nested
    class LatestOf {

        @Test
        void latestOfReturnsTheNewestEventAmongTheGivenTypes() {
            // given — the withdrawal is the newest of the two requested types
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(5)), a1Tag());

            // when
            Object latest = a1Scope().latestOf(MoneyDeposited.class, MoneyWithdrawn.class);

            // then
            assertThat(latest).isInstanceOfSatisfying(MoneyWithdrawn.class,
                                                       w -> assertThat(w.amount()).isEqualByComparingTo("5"));
        }

        @Test
        void latestOfIgnoresEventsOutsideTheRequestedTypes() {
            // given — a closure is the chronologically newest event but is not among the requested types
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(5)), a1Tag());
            seed(new AccountClosed("a1"), a1Tag());

            // when — only deposit/withdrawal are considered
            Object latest = a1Scope().latestOf(MoneyDeposited.class, MoneyWithdrawn.class);

            // then — the withdrawal wins, the later closure is skipped
            assertThat(latest).isInstanceOf(MoneyWithdrawn.class);
        }

        @Test
        void latestOfReturnsNullWhenNoneOfTheRequestedTypesOccurred() {
            // given — only an unrelated event
            seed(new AccountClosed("a1"), a1Tag());

            // when / then
            assertThat(a1Scope().latestOf(MoneyDeposited.class, MoneyWithdrawn.class)).isNull();
        }

        @Test
        void latestOfRejectsAnEmptyTypeList() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());

            // when / then
            assertThatThrownBy(() -> a1Scope().latestOf())
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class First {

        @Test
        void firstReturnsTheEarliestMatchingPayload() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(20)), a1Tag());

            // when / then
            assertThat(a1Scope().first(MoneyDeposited.class))
                    .hasValueSatisfying(e -> assertThat(e.amount()).isEqualByComparingTo("10"));
        }

        @Test
        void firstReturnsEmptyWhenNoMatch() {
            // given — only an unrelated event
            seed(new AccountClosed("a1"), a1Tag());

            // when / then
            assertThat(a1Scope().first(MoneyDeposited.class)).isEmpty();
        }
    }

    @Nested
    class Count {

        @Test
        void countSumsEventsAcrossAllRequestedTypes() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(50)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(20)), a1Tag());

            // when / then
            assertThat(a1Scope().count(MoneyDeposited.class, MoneyWithdrawn.class)).isEqualTo(3L);
        }

        @Test
        void countOverASingleTypeMatchesOnlyEventsOfThatType() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(20)), a1Tag());

            // when / then
            assertThat(a1Scope().count(MoneyDeposited.class)).isEqualTo(1L);
        }

        @Test
        void countReturnsZeroWhenNoEventsMatch() {
            // given — no events seeded

            // when / then
            assertThat(a1Scope().count(MoneyDeposited.class)).isZero();
        }

        @Test
        void countRejectsAnEmptyTypeList() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());

            // when / then
            assertThatThrownBy(() -> a1Scope().count())
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class Total {

        @Test
        void totalAggregatesTheTypedProjection() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(50)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(25)), a1Tag());

            // when / then
            assertThat(a1Scope().total(MoneyDeposited.class, MoneyDeposited::amount))
                    .isEqualByComparingTo("175");
        }

        @Test
        void totalReturnsZeroWhenNoMatchingEvents() {
            // given — only an unrelated event
            seed(new AccountClosed("a1"), a1Tag());

            // when / then
            assertThat(a1Scope().total(MoneyDeposited.class, MoneyDeposited::amount))
                    .isEqualByComparingTo("0");
        }

        @Test
        void totalMinusTotalComputesBalanceAcrossOneScope() {
            // given — the banking balance example from the ADR
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(50)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(30)), a1Tag());

            // when
            History account = a1Scope();
            BigDecimal balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
                                        .subtract(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));

            // then
            assertThat(balance).isEqualByComparingTo("120");
        }
    }

    @Nested
    class EntryReads {

        @Test
        void entryReturnsTheLatestMatchingPayloadWithItsTimestamp() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(20)), a1Tag());

            // when / then — the entry carries both the latest payload and a recorded timestamp
            assertThat(a1Scope().entry(MoneyDeposited.class))
                    .hasValueSatisfying(entry -> {
                        assertThat(entry.payload().amount()).isEqualByComparingTo("20");
                        assertThat(entry.occurredAt()).isNotNull();
                    });
        }

        @Test
        void entryReturnsEmptyWhenNoMatch() {
            // given — only an unrelated event
            seed(new AccountClosed("a1"), a1Tag());

            // when / then
            assertThat(a1Scope().entry(MoneyDeposited.class)).isEmpty();
        }
    }

    @Nested
    class AdvancedMatching {

        @Test
        void matchingNarrowsToTheGivenCriteria() {
            // given — events under two distinct account tags
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), Set.of(new Tag("account", "a1")));
            seed(new MoneyDeposited("a2", BigDecimal.valueOf(200)), Set.of(new Tag("account", "a2")));

            // when — narrow with an explicit criteria rather than of(...)
            EventCriteria a2 = EventCriteria.havingTags(Tag.of("account", "a2"));
            History scope = rootHistory().matching(a2);

            // then — only the a2 events are visible
            assertThat(scope.total(MoneyDeposited.class, MoneyDeposited::amount)).isEqualByComparingTo("200");
        }

        @Test
        void matchingAndOfNarrowToTheSameScope() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());

            // when — of("account","a1") is sugar over matching(havingTags(...))
            BigDecimal viaOf = a1Scope().total(MoneyDeposited.class, MoneyDeposited::amount);
            BigDecimal viaMatching = rootHistory()
                    .matching(EventCriteria.havingTags(Tag.of("account", "a1")))
                    .total(MoneyDeposited.class, MoneyDeposited::amount);

            // then
            assertThat(viaOf).isEqualByComparingTo("100");
            assertThat(viaMatching).isEqualByComparingTo(viaOf);
        }
    }

    @Nested
    class Scoping {

        @Test
        void distinctScopesSeeOnlyTheirOwnEvents() {
            // given — events under two different account tags
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), Set.of(new Tag("account", "a1")));
            seed(new MoneyDeposited("a2", BigDecimal.valueOf(200)), Set.of(new Tag("account", "a2")));

            // when
            History root = rootHistory();
            BigDecimal a1 = root.of("account", "a1").total(MoneyDeposited.class, MoneyDeposited::amount);
            BigDecimal a2 = root.of("account", "a2").total(MoneyDeposited.class, MoneyDeposited::amount);

            // then
            assertThat(a1).isEqualByComparingTo("100");
            assertThat(a2).isEqualByComparingTo("200");
        }
    }

    @Nested
    class Guards {

        @Test
        void readingTheUnboundRootFailsFast() {
            // given — an event exists, but the root history is never narrowed
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());

            // when / then — every read on the root must be rejected as a programming error
            History root = rootHistory();
            assertThatThrownBy(() -> root.has(MoneyDeposited.class)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> root.never(MoneyDeposited.class)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> root.lastWas(MoneyDeposited.class)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> root.latest(MoneyDeposited.class)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> root.latestOf(MoneyDeposited.class)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> root.first(MoneyDeposited.class)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> root.count(MoneyDeposited.class)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> root.total(MoneyDeposited.class, MoneyDeposited::amount))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> root.entry(MoneyDeposited.class)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void narrowingAnAlreadyNarrowedHistoryFailsFast() {
            // given — a narrowed scope
            History account = a1Scope();

            // when / then — a second of(...) is a programming error
            assertThatThrownBy(() -> account.of("account", "a2"))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void matchingAnAlreadyNarrowedHistoryFailsFast() {
            // given — a narrowed scope
            History account = a1Scope();

            // when / then — a second matching(...) is equally rejected
            assertThatThrownBy(() -> account.matching(EventCriteria.havingTags(Tag.of("account", "a2"))))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    // ----------------------------------------------------------------------
    // Test fixtures
    // ----------------------------------------------------------------------

    record MoneyDeposited(String accountId, BigDecimal amount) {
    }

    record MoneyWithdrawn(String accountId, BigDecimal amount) {
    }

    record AccountClosed(String accountId) {
    }
}
