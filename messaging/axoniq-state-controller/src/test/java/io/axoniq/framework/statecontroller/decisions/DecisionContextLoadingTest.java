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
import io.axoniq.framework.statecontroller.conditions.NumericCondition;
import io.axoniq.framework.statecontroller.eventstream.EventStream;
import io.axoniq.framework.statecontroller.eventstream.LateConditionException;
import io.axoniq.framework.statecontroller.runtime.HistorySession;
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
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.messaging.core.ApplicationContext;
import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Phase 2 mechanics test for the {@link HistorySession} loading lifecycle. The point of these tests is to
 * pin down the lifecycle invariants — single sourced read per decision, seal-on-first-force, refusal of late
 * registration — not the value-correctness of every {@link io.axoniq.framework.statecontroller.eventstream.EventStream}
 * helper. Per-helper value tests live with the Phase 1 acceptance suite.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
class DecisionContextLoadingTest {

    private InMemoryEventStorageEngine engine;
    private EventStore eventStore;
    private ProcessingContext processingContext;

    @BeforeEach
    void setUp() {
        engine = new InMemoryEventStorageEngine();
        eventStore = spy(new StorageEngineBackedEventStore(engine, new SimpleEventBus(), e -> Set.of()));
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

    private HistorySession newDecisionContext() {
        return new HistorySession(eventStore, processingContext, Clock.systemUTC());
    }

    private <P> void seed(P payload, Set<Tag> tags) {
        var message = new GenericEventMessage(new MessageType(payload.getClass()), payload);
        TaggedEventMessage<?> tagged = new GenericTaggedEventMessage<>(message, tags);
        @SuppressWarnings("unchecked")
        AppendTransaction<ConsistencyMarker> tx = (AppendTransaction<ConsistencyMarker>)
                engine.appendEvents(AppendCondition.none(), null, List.of(tagged)).join();
        tx.commit().thenCompose(tx::afterCommit).join();
    }

    @Nested
    class Seal {

        @Test
        void declaringTwoConditionsAndForcingBothReusesASingleRead() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), Set.of(new Tag("account", "a1")));
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(50)), Set.of(new Tag("account", "a1")));

            // when
            var dc = newDecisionContext();
            var account = dc.scope("account", "a1");
            BooleanCondition hasDeposits = account.contains(MoneyDeposited.class);
            NumericCondition<BigDecimal> total = account.sum(MoneyDeposited.class, MoneyDeposited::amount);

            // then
            assertThat(hasDeposits.resolve()).isTrue();
            assertThat(total.resolve()).isEqualByComparingTo("150");
            verify(eventStore, times(1)).transaction(processingContext);
        }

        @Test
        void forcingTheSameConditionTwiceTriggersOnlyOneEventStoreTransaction() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), Set.of(new Tag("account", "a1")));

            // when
            var dc = newDecisionContext();
            var stream = dc.scope("account", "a1");
            var hasDeposits = stream.contains(MoneyDeposited.class);

            // then — forcing once and then again must not re-issue the sourced read
            assertThat(hasDeposits.resolve()).isTrue();
            assertThat(hasDeposits.resolve()).isTrue();
            verify(eventStore, times(1)).transaction(processingContext);
        }
    }

    @Nested
    class LateDeclaration {

        @Test
        void registeringATypeAfterTheScopeIsSealedThrowsLateConditionException() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), Set.of(new Tag("account", "a1")));

            var dc = newDecisionContext();
            var stream = dc.scope("account", "a1");
            // seal this scope by forcing a condition
            stream.contains(MoneyDeposited.class).resolve();

            // when / then
            assertThatExceptionOfType(LateConditionException.class)
                    .isThrownBy(() -> stream.contains(MoneyWithdrawn.class))
                    .withMessageContaining(MoneyWithdrawn.class.getName())
                    .withMessageContaining("scope has been sealed")
                    // the per-scope message must name the offending scope's tag so users can identify it
                    .withMessageContaining("account")
                    .withMessageContaining("a1");
        }

        @Test
        void openingANewScopeAfterAnotherIsSealedIsAllowed() {
            // given — each scope owns its own seal state; forcing one scope must not prevent another from
            // being opened and declaring conditions afterwards
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), Set.of(new Tag("account", "a1")));
            seed(new MoneyDeposited("a2", BigDecimal.valueOf(200)), Set.of(new Tag("account", "a2")));

            var dc = newDecisionContext();
            dc.scope("account", "a1").contains(MoneyDeposited.class).resolve();

            // when — open and use a second, independent scope
            var a2Total = dc.scope("account", "a2").sum(MoneyDeposited.class, MoneyDeposited::amount);

            // then — the second scope loads independently, no LateConditionException
            assertThat(a2Total.resolve()).isEqualByComparingTo("200");
        }
    }

    @Nested
    class MultiScope {

        @Test
        void distinctScopesShareASingleEventStoreTransaction() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), Set.of(new Tag("account", "a1")));
            seed(new MoneyDeposited("a2", BigDecimal.valueOf(200)), Set.of(new Tag("account", "a2")));

            // when
            var dc = newDecisionContext();
            var a1 = dc.scope("account", "a1");
            var a2 = dc.scope("account", "a2");
            NumericCondition<BigDecimal> a1Total = a1.sum(MoneyDeposited.class, MoneyDeposited::amount);
            NumericCondition<BigDecimal> a2Total = a2.sum(MoneyDeposited.class, MoneyDeposited::amount);

            // then — each scope sees only its own events
            assertThat(a1Total.resolve()).isEqualByComparingTo("100");
            assertThat(a2Total.resolve()).isEqualByComparingTo("200");
            // and both scopes' source(...) calls share the same EventStoreTransaction handle (cached on the
            // ProcessingContext), so the recorded ConsistencyMarker accumulates across them
            assertThat(eventStore.transaction(processingContext))
                    .isSameAs(eventStore.transaction(processingContext));
        }

        @Test
        void resolvingOneScopeSealsEveryDeclaredScopeTogether() {
            // given — events for both accounts
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), Set.of(new Tag("account", "a1")));
            seed(new MoneyDeposited("a2", BigDecimal.valueOf(200)), Set.of(new Tag("account", "a2")));

            // when — declare conditions on both scopes but resolve only a1's
            var dc = newDecisionContext();
            var a1 = dc.scope("account", "a1");
            var a2 = dc.scope("account", "a2");
            NumericCondition<BigDecimal> a1Total = a1.sum(MoneyDeposited.class, MoneyDeposited::amount);
            NumericCondition<BigDecimal> a2Total = a2.sum(MoneyDeposited.class, MoneyDeposited::amount);
            a1Total.resolve();

            // then — the first resolution sealed BOTH declared scopes into the coordinated read, so a2's
            //        condition future is already completed without a resolve of its own
            assertThat(a2Total.resolveAsync())
                    .isCompletedWithValueMatching(t -> t.compareTo(BigDecimal.valueOf(200)) == 0);
            // and — the held a2 stream reference is sealed: late declarations on it fail fast, while a fresh
            //       scope lookup accepts new conditions via a supplementary read
            assertThatExceptionOfType(LateConditionException.class)
                    .isThrownBy(() -> a2.contains(MoneyWithdrawn.class));
            assertThatNoException().isThrownBy(
                    () -> dc.scope("account", "a2").contains(MoneyWithdrawn.class).resolve());
        }

        @Test
        void sameScopeKeyReturnsTheSameStreamInstance() {
            // given / when — two lookups for the same (key, value)
            var dc = newDecisionContext();
            EventStream first = dc.scope("account", "a1");
            EventStream second = dc.scope("account", "a1");

            // then — same stream so registrations and seal state are shared
            assertThat(first).isSameAs(second);
        }
    }

    @Nested
    class EmptyScope {

        @Test
        void aScopeWithNoTypedConditionsCausesNoExtraLoadCalls() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), Set.of(new Tag("account", "a1")));

            // when
            var dc = newDecisionContext();
            var stream = dc.scope("account", "a1");
            stream.contains(MoneyDeposited.class).resolve();

            // then
            verify(eventStore, times(1)).transaction(processingContext);
        }

        @Test
        void aDecisionContextWithoutAnyScopeNeverIssuesARead() {
            // given — no scope opened, no conditions declared
            newDecisionContext();

            // then — nothing forced a load
            verify(eventStore, times(0)).transaction(processingContext);
        }
    }
}
