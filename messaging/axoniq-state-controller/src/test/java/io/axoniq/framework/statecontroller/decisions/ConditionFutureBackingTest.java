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
import io.axoniq.framework.statecontroller.eventstream.EventCondition;
import io.axoniq.framework.statecontroller.eventstream.EventStream;
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
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Tests pinning down the future-backed shape of {@link Condition}: that
 * {@link Condition#asCompletableFuture() asCompletableFuture()} is the canonical operation and that the
 * synchronous {@link Condition#value() value()} bridge composes correctly with it.
 * <p>
 * These cases exercise paths that were either underspecified or fragile under the previous
 * {@code seal + awaitLoaded + finalValue} design:
 * <ul>
 *     <li><strong>Projection without awaiting the parent.</strong> Pre-refactor, calling
 *         {@code eventCondition.isA(X).isTrue()} without first forcing {@code eventCondition} itself triggered
 *         {@code seal()} but skipped the load-await, racing the projection against the still-running reduce.
 *         The future-backed shape eliminates the race because projections chain off the selection-future.</li>
 *     <li><strong>Direct async composition.</strong> Callers wanting to fan-out their own continuations should
 *         be able to chain {@code thenApply} on {@code asCompletableFuture()} and observe the completion as
 *         soon as the underlying reduce finishes.</li>
 *     <li><strong>Exception propagation.</strong> When event processing fails, every registered condition's
 *         future must complete exceptionally, and {@link Condition#value() value()} must re-raise the cause
 *         through {@code FutureUtils.joinAndUnwrap}'s sneaky-throw.</li>
 * </ul>
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
class ConditionFutureBackingTest {

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
    class ProjectionWithoutAwaitingParent {

        @Test
        void isAReturnsCorrectBooleanWithoutForcingTheParentEventConditionFirst() {
            // given — two candidate events for the latestOf(...) selection
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new AccountClosed("a1"), a1Tag());

            // when — derive a type-only projection and force it WITHOUT first forcing the parent.
            // Pre-refactor this code path raced the projection against the still-running reduce (seal but no
            // await). Post-refactor the projection chains off the selection-future, so the future itself
            // encodes the await.
            EventCondition latest = newDecisionContext()
                    .scope("account", "a1")
                    .latestOf(MoneyDeposited.class, AccountClosed.class);
            BooleanCondition isClosed = latest.isA(AccountClosed.class);

            // then — projection observes the correct selected event (AccountClosed was last)
            assertThat(isClosed.isTrue()).isTrue();
        }

        @Test
        void asUnwrapsToTheCorrectPayloadWithoutForcingTheParentEventConditionFirst() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyWithdrawn("a1", BigDecimal.valueOf(20)), a1Tag());

            // when — same idea, but with the payload-returning as(...) projection
            EventCondition latest = newDecisionContext()
                    .scope("account", "a1")
                    .latestOf(MoneyDeposited.class, MoneyWithdrawn.class);

            // then — payload extraction completes correctly even though the parent was never forced directly
            assertThat(latest.as(MoneyWithdrawn.class).orDefault(null).value())
                    .isInstanceOf(MoneyWithdrawn.class)
                    .extracting("amount")
                    .isEqualTo(BigDecimal.valueOf(20));
        }
    }

    @Nested
    class DirectFutureComposition {

        @Test
        void asCompletableFutureCompletesWithTheConditionValueWhenForced() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(50)), a1Tag());

            // when — obtain the future and chain a thenApply BEFORE blocking
            var sum = newDecisionContext()
                    .scope("account", "a1")
                    .sum(MoneyDeposited.class, MoneyDeposited::amount);
            CompletableFuture<String> derived = sum.asCompletableFuture()
                                                   .thenApply(BigDecimal::toPlainString);

            // then — the chained continuation observes the reduced value
            assertThat(derived.join()).isEqualTo("150");
        }

        @Test
        void multipleConditionsExposeIndependentFuturesThatAllComplete() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());
            seed(new AccountClosed("a1"), a1Tag());

            // when — declare BOTH conditions before forcing either, so the same sealed read covers both.
            // Then obtain the futures and join them; they must both complete from the single underlying reduce.
            EventStream account = newDecisionContext().scope("account", "a1");
            BooleanCondition closed = account.contains(AccountClosed.class);
            var total = account.sum(MoneyDeposited.class, MoneyDeposited::amount);

            CompletableFuture<Boolean> closedFuture = closed.asCompletableFuture();
            CompletableFuture<BigDecimal> totalFuture = total.asCompletableFuture();

            // then — both futures are completed by the same underlying reduce
            assertThat(closedFuture.join()).isTrue();
            assertThat(totalFuture.join()).isEqualByComparingTo("100");
        }
    }

    @Nested
    class ExceptionPropagation {

        @Test
        void aFailingReducerCompletesEveryRegisteredConditionExceptionally() {
            // given — one event in scope; we register a fold whose user-supplied reducer throws on the first
            // matching event. Because the reduce runs over the stream's accumulator list, the failure
            // propagates out of MessageStream.reduce(...) and through whenComplete(...) into every
            // registered SourcedCondition's future.
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());

            EventStream account = newDecisionContext().scope("account", "a1");
            // A second, well-behaved condition on the same scope must also receive the failure.
            BooleanCondition hasDeposit = account.contains(MoneyDeposited.class);
            Condition<Integer> failing = account.<Integer>fold(0)
                                                .event(MoneyDeposited.class, (acc, evt) -> {
                                                    throw new IllegalStateException("reducer blew up");
                                                });

            // when / then — the failing reducer's own value() rethrows the original cause
            assertThatExceptionOfType(IllegalStateException.class)
                    .isThrownBy(failing::value)
                    .withMessage("reducer blew up");

            // and the sibling condition on the same scope is also failed by the fan-out — value() must
            // surface the same root cause rather than hang or return a stale default.
            assertThatExceptionOfType(IllegalStateException.class)
                    .isThrownBy(hasDeposit::value)
                    .withMessage("reducer blew up");
        }

        @Test
        void asCompletableFutureExposesTheFailureViaCompletionException() {
            // given — failing reducer wired the same way
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)), a1Tag());

            Condition<Integer> failing = newDecisionContext()
                    .scope("account", "a1")
                    .<Integer>fold(0)
                    .event(MoneyDeposited.class, (acc, evt) -> {
                        throw new IllegalStateException("reducer blew up");
                    });

            // when — chain a thenApply that should never run
            CompletableFuture<Integer> doubled = failing.asCompletableFuture().thenApply(i -> i * 2);

            // then — the async path wraps the original cause in CompletionException (standard CF semantics);
            // the synchronous value() path unwraps it via joinAndUnwrap (verified in the test above)
            assertThatExceptionOfType(CompletionException.class)
                    .isThrownBy(doubled::join)
                    .withCauseInstanceOf(IllegalStateException.class);
        }
    }
}
