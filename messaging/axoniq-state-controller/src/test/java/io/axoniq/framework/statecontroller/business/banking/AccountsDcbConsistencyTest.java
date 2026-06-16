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

import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine.AppendTransaction;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The marquee Dynamic Consistency Boundary (DCB) test: it proves that a {@code @Decide} round-trip
 * (read history, then conditionally append) is <em>rejected</em> when a conflicting event lands in the same
 * account scope after the read captured its consistency marker — the safety property that makes the cross-entity
 * decision in {@link Accounts} correct under concurrency.
 *
 * <h2>What this test covers, and at which level</h2>
 * The conflict rejection cannot be reproduced through a {@code History} read on a {@link StubProcessingContext}
 * (the skeleton used by {@code HistoryValueTest} / {@code HistoryCriteriaTest}). The reason is structural:
 * the guarded, conflict-detecting append happens during the <em>commit</em> lifecycle of a real
 * {@link ProcessingContext}; {@code History.materialized()} only sources events and never appends, while
 * {@code DecisionDispatch} appends via an {@code EventAppender} whose guarded append is then run by the
 * unit-of-work commit. A {@link StubProcessingContext} stores lifecycle actions but never drives the
 * prepare-commit phase, so no guarded append is ever attempted there.
 * <p>
 * This test therefore reproduces the boundary at the most faithful reliable level: the
 * {@link org.axonframework.eventsourcing.eventstore.EventStorageEngine EventStorageEngine} /
 * {@link AppendCondition} layer, mirroring <em>exactly</em>
 * <ul>
 *   <li>how {@code SourcedHistory.materialized()} sources — it sources a {@link SourcingCondition} on the active
 *       {@link EventStoreTransaction}, which records the {@link ConsistencyMarker} captured at read time
 *       ({@link EventStoreTransaction#appendPosition()}); and</li>
 *   <li>how the decision append is guarded — {@code DefaultEventStoreTransaction} registers, on prepare-commit, an
 *       {@code appendEvents(AppendCondition.withCriteria(scope).withMarker(marker), ...)} with the marker captured
 *       during sourcing.</li>
 * </ul>
 * Running the guarded append with the <em>stale</em> captured marker, after a conflicting event has been
 * committed into the same scope, fails the append future with
 * {@link AppendEventsTransactionRejectedException}. The rejection surfaces on the {@code appendEvents(...)}
 * future (not on {@code commit()}); the in-memory engine returns a failed future before handing back a
 * transaction.
 * <p>
 * <strong>NOT covered here:</strong> the end-to-end wiring through the {@code @Decide} handler enhancer, the
 * {@code History} parameter resolver, and the live unit-of-work commit (that path is exercised by
 * {@code AccountsTest} for the success / business-rejection cases). This test isolates the underlying DCB
 * mechanism that those layers delegate to, and does not re-test their wiring.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class AccountsDcbConsistencyTest {

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

    @Nested
    class CrossAccountAppendConflict {

        @Test
        void aGuardedAppendIsRejectedWhenAConflictingEventLandsAfterTheMarkerWasCaptured() {
            // given — an initial event under the "account/a1" scope
            EventCriteria scope = EventCriteria.havingTags(Tag.of("account", "a1"));
            seed(new MoneyDeposited("a1", new BigDecimal("100")));

            // (a) SOURCE the criteria on the active EventStoreTransaction so the ConsistencyMarker is captured —
            //     exactly what SourcedHistory.materialized() does: sourcing through eventStore.transaction(ctx)
            //     records the marker, no manual marker threading. The stream is fully consumed so the marker is
            //     recorded on stream completion.
            ConsistencyMarker capturedMarker = sourceAndCaptureMarker(scope);

            // (b) COMMIT a CONFLICTING event into the SAME scope via a separate, unconditional transaction.
            commitUnconditional(taggedWithdrawn("a1", "50"));

            // (c) ATTEMPT the guarded append with the STALE captured marker — this mirrors what
            //     DefaultEventStoreTransaction registers on prepare-commit: withCriteria(scope).withMarker(marker).
            AppendCondition guarded = AppendCondition.withCriteria(scope).withMarker(capturedMarker);
            CompletableFuture<AppendTransaction<?>> rejected =
                    engine.appendEvents(guarded, null, List.of(taggedWithdrawn("a1", "10")));

            // then — the rejection surfaces on the appendEvents(...) future, NOT on commit()
            assertThat(rejected)
                    .failsWithin(Duration.ofSeconds(1))
                    .withThrowableThat()
                    .havingCause()
                    .isInstanceOf(AppendEventsTransactionRejectedException.class);
        }

        @Test
        void aGuardedAppendSucceedsWhenNoConflictingEventLandsAfterTheMarkerWasCaptured() {
            // given — an initial event under the "account/a1" scope
            EventCriteria scope = EventCriteria.havingTags(Tag.of("account", "a1"));
            seed(new MoneyDeposited("a1", new BigDecimal("100")));

            // when — the marker is captured and no conflicting event is committed in between
            ConsistencyMarker capturedMarker = sourceAndCaptureMarker(scope);
            AppendCondition guarded = AppendCondition.withCriteria(scope).withMarker(capturedMarker);

            // then — the guarded append is accepted (the boundary is intact) and commits cleanly
            AppendTransaction<?> tx = FutureUtils.joinAndUnwrap(
                    engine.appendEvents(guarded, null, List.of(taggedWithdrawn("a1", "10"))),
                    Duration.ofSeconds(1));
            assertThat(commit(tx)).isNotNull();
        }

        @Test
        void aGuardedAppendOnAForeignScopeIsUnaffectedByAConflictInAnotherAccount() {
            // given — markers captured for two distinct account scopes
            EventCriteria a1 = EventCriteria.havingTags(Tag.of("account", "a1"));
            EventCriteria a2 = EventCriteria.havingTags(Tag.of("account", "a2"));
            seed(new MoneyDeposited("a1", new BigDecimal("100")));
            seed(new MoneyDeposited("a2", new BigDecimal("200")));

            ConsistencyMarker a2Marker = sourceAndCaptureMarker(a2);

            // when — a conflicting event lands in the a1 scope only
            commitUnconditional(taggedWithdrawn("a1", "50"));

            // then — the a2-scoped guarded append is NOT rejected: the DCB boundary is per-scope, so a conflict
            //        in a1 leaves the a2 marker valid
            AppendCondition guardedA2 = AppendCondition.withCriteria(a2).withMarker(a2Marker);
            AppendTransaction<?> tx = FutureUtils.joinAndUnwrap(
                    engine.appendEvents(guardedA2, null, List.of(taggedWithdrawn("a2", "20"))),
                    Duration.ofSeconds(1));
            assertThat(commit(tx)).isNotNull();
        }
    }

    // ----------------------------------------------------------------------
    // Helpers — mirror SourcedHistory's source path and DecisionDispatch's append path
    // ----------------------------------------------------------------------

    /**
     * Sources {@code scope} on the active {@link EventStoreTransaction}, fully consuming the stream so the
     * {@link ConsistencyMarker} is recorded, then returns it via {@link EventStoreTransaction#appendPosition()}.
     */
    private ConsistencyMarker sourceAndCaptureMarker(EventCriteria scope) {
        SourcingCondition sourcing = SourcingCondition.conditionFor(scope);
        EventStoreTransaction tx = eventStore.transaction(processingContext);
        FutureUtils.joinAndUnwrap(
                tx.source(sourcing).reduce(new ArrayList<EventMessage>(), (list, entry) -> {
                    list.add(entry.message());
                    return list;
                }),
                Duration.ofSeconds(5));
        return tx.appendPosition();
    }

    private void seed(MoneyDeposited payload) {
        commitUnconditional(taggedDeposited(payload.accountId(), payload.amount().toPlainString()));
    }

    private void commitUnconditional(TaggedEventMessage<?> tagged) {
        AppendTransaction<?> tx = FutureUtils.joinAndUnwrap(
                engine.appendEvents(AppendCondition.none(), null, List.of(tagged)),
                Duration.ofSeconds(1));
        commit(tx);
    }

    @SuppressWarnings("unchecked")
    private static ConsistencyMarker commit(AppendTransaction<?> tx) {
        AppendTransaction<ConsistencyMarker> typed = (AppendTransaction<ConsistencyMarker>) tx;
        return FutureUtils.joinAndUnwrap(
                typed.commit().thenCompose(typed::afterCommit),
                Duration.ofSeconds(1));
    }

    private static TaggedEventMessage<?> taggedDeposited(String accountId, String amount) {
        return tagged(new MoneyDeposited(accountId, new BigDecimal(amount)), accountId);
    }

    private static TaggedEventMessage<?> taggedWithdrawn(String accountId, String amount) {
        return tagged(new MoneyWithdrawn(accountId, new BigDecimal(amount)), accountId);
    }

    private static TaggedEventMessage<?> tagged(Object payload, String accountId) {
        var message = new GenericEventMessage(new MessageType(payload.getClass()), payload);
        return new GenericTaggedEventMessage<>(message, Set.of(new Tag("account", accountId)));
    }
}
