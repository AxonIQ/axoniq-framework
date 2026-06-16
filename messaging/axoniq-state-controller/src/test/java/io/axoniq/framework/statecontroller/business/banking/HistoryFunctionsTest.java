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

/**
 * A guided tour of the {@link History} read surface, exercised directly against a seeded account history so each
 * function's behaviour is visible in isolation. The seeded {@code a1} history is, in order: deposit 100, deposit
 * 50, withdraw 30 (net balance 120, newest movement a withdrawal); {@code a2} holds a single deposit of 200.
 * <p>
 * Demonstrates the scope builders — {@link History#matching(EventCriteria) matching(...)} (the power-user
 * escape hatch, both tag-only and type-restricted), {@link History#of(String, Object) of(tag)},
 * {@link History#of(String, Object, Class[]) of(tag, types)}, {@link History#of(Class[]) of(types)},
 * {@link History#and(Class[]) and(...)}, {@link History#or(String, Object) or(tag)} and {@link History#or(Class[])
 * or(types)} — and the readers {@link History#has has} / {@link History#never never},
 * {@link History#count count}, {@link History#total total}, {@link History#latest latest} /
 * {@link History#first first} / {@link History#entry entry}, {@link History#lastWas lastWas}, and
 * {@link History#latestOf latestOf} (branched with {@code instanceof}).
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class HistoryFunctionsTest {

    private InMemoryEventStorageEngine engine;
    private EventStore eventStore;
    private MessageTypeResolver typeResolver;
    private ProcessingContext processingContext;

    @BeforeEach
    void setUp() {
        engine = new InMemoryEventStorageEngine();
        eventStore = new StorageEngineBackedEventStore(engine, new SimpleEventBus(), e -> Set.of());
        typeResolver = new ClassBasedMessageTypeResolver();
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

        // a1: deposit 100, deposit 50, withdraw 30  (balance 120, newest movement is a withdrawal)
        seed(new MoneyDeposited("a1", new BigDecimal("100")), accountTag("a1"));
        seed(new MoneyDeposited("a1", new BigDecimal("50")), accountTag("a1"));
        seed(new MoneyWithdrawn("a1", new BigDecimal("30")), accountTag("a1"));
        // a2: a single deposit of 200
        seed(new MoneyDeposited("a2", new BigDecimal("200")), accountTag("a2"));
    }

    private History rootHistory() {
        return HistoryFactory.rootHistoryFor(processingContext);
    }

    @Nested
    class Matching {

        @Test
        void tagOnlyCriteriaLoadsEveryEventTypeForTheTag() {
            // given the power-user escape hatch with a hand-built, tag-only criteria
            History account = rootHistory().matching(EventCriteria.havingTags(Tag.of("account", "a1")));

            // then every event tagged for a1 is in scope, regardless of type
            assertThat(account.count(MoneyDeposited.class, MoneyWithdrawn.class)).isEqualTo(3L);
            assertThat(account.has(MoneyWithdrawn.class)).isTrue();
        }

        @Test
        void typeRestrictedCriteriaExcludesUnlistedTypes() {
            // given a criteria narrowed to deposits only, resolving the class through the same resolver
            History deposits = rootHistory().matching(
                    EventCriteria.havingTags(Tag.of("account", "a1"))
                                 .andBeingOneOfTypes(typeResolver, MoneyDeposited.class));

            // then withdrawals fall outside the boundary, only the two deposits are seen
            assertThat(deposits.count(MoneyDeposited.class)).isEqualTo(2L);
            assertThat(deposits.has(MoneyWithdrawn.class)).isFalse();
        }
    }

    @Nested
    class Builders {

        @Test
        void ofTagThenAndRestrictsToTypes() {
            // of(tag).and(types) — a type-precise single-scope boundary
            History account = rootHistory().of("account", "a1").and(MoneyDeposited.class);
            assertThat(account.count(MoneyDeposited.class)).isEqualTo(2L);
            assertThat(account.has(MoneyWithdrawn.class)).isFalse();
        }

        @Test
        void taglessOfTypesMatchesAcrossAllTags() {
            // of(types) — tagless, matches that type under every account
            History allDeposits = rootHistory().of(MoneyDeposited.class);
            assertThat(allDeposits.count(MoneyDeposited.class)).isEqualTo(3L); // two on a1, one on a2
        }

        @Test
        void orUnionsTwoTagScopes() {
            // of(tag).or(tag) — the union of two accounts in one scope
            History both = rootHistory().of("account", "a1").or("account", "a2");
            assertThat(both.count(MoneyDeposited.class, MoneyWithdrawn.class)).isEqualTo(4L);
            assertThat(both.total(MoneyDeposited.class, MoneyDeposited::amount)).isEqualByComparingTo("350");
        }

        @Test
        void orUnionsTwoTaglessTypeTerms() {
            // of(types).or(types) — tagless union of two event types across all tags
            History movements = rootHistory().of(MoneyDeposited.class).or(MoneyWithdrawn.class);
            assertThat(movements.count(MoneyDeposited.class, MoneyWithdrawn.class)).isEqualTo(4L);
        }
    }

    @Nested
    class Readers {

        @Test
        void countAndTotalFoldOverTheScope() {
            History account = rootHistory().of("account", "a1");
            assertThat(account.count(MoneyDeposited.class)).isEqualTo(2L);
            // total deposits 100 + 50, minus the withdrawal 30, is the net balance
            BigDecimal balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
                                        .subtract(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));
            assertThat(balance).isEqualByComparingTo("120");
        }

        @Test
        void firstAndLatestSelectByAge() {
            History account = rootHistory().of("account", "a1");
            assertThat(account.first(MoneyDeposited.class)).hasValueSatisfying(
                    d -> assertThat(d.amount()).isEqualByComparingTo("100")); // oldest deposit
            assertThat(account.latest(MoneyDeposited.class)).hasValueSatisfying(
                    d -> assertThat(d.amount()).isEqualByComparingTo("50"));  // newest deposit
        }

        @Test
        void entryCarriesTheRecordedTimestamp() {
            History account = rootHistory().of("account", "a1");
            assertThat(account.entry(MoneyWithdrawn.class)).hasValueSatisfying(e -> {
                assertThat(e.payload().amount()).isEqualByComparingTo("30");
                assertThat(e.occurredAt()).isNotNull();
            });
        }

        @Test
        void lastWasAndLatestOfReadTheNewestMovement() {
            History account = rootHistory().of("account", "a1");
            // the newest event in the scope is the withdrawal
            assertThat(account.lastWas(MoneyWithdrawn.class)).isTrue();
            // latestOf returns the bare payload of the newest among the listed types, for instanceof / switch
            Object newest = account.latestOf(MoneyDeposited.class, MoneyWithdrawn.class);
            assertThat(newest).isInstanceOf(MoneyWithdrawn.class);
        }

        @Test
        void hasAndNeverProbeMembership() {
            History account = rootHistory().of("account", "a1");
            assertThat(account.has(MoneyDeposited.class)).isTrue();
            assertThat(account.never(AccountClosed.class)).isTrue(); // a1 was never closed
        }
    }

    private <P> void seed(P payload, Set<Tag> tags) {
        var message = new GenericEventMessage(new MessageType(payload.getClass()), payload);
        TaggedEventMessage<?> tagged = new GenericTaggedEventMessage<>(message, tags);
        @SuppressWarnings("unchecked")
        AppendTransaction<ConsistencyMarker> tx = (AppendTransaction<ConsistencyMarker>)
                engine.appendEvents(AppendCondition.none(), null, List.of(tagged)).join();
        tx.commit().thenCompose(tx::afterCommit).join();
    }

    private static Set<Tag> accountTag(String accountId) {
        return Set.of(new Tag("account", accountId));
    }
}
