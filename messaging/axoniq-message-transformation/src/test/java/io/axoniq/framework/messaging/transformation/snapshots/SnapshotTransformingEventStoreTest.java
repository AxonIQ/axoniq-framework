/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.snapshots;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPositions;
import org.axonframework.eventsourcing.eventstore.SnapshotCapableEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.eventsourcing.handler.InitializingEntityEvolver;
import org.axonframework.eventsourcing.handler.SnapshottingEntityLifecycleHandler;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.api.SnapshotCandidate;
import org.axonframework.eventsourcing.snapshot.api.SnapshotMatch;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link SnapshotTransformingEventStore}, sourcing an entity through a
 * {@link SnapshottingEntityLifecycleHandler} from snapshots stored under earlier versions, and from snapshots taken by
 * Axon Framework 4.
 *
 * @author John Hendrikx
 */
class SnapshotTransformingEventStoreTest {

    record Deposited(@EventTag(key = "account") String id, long amount) {}

    record Account(String id, long balance) {}

    record AccountV1(String id, long cents) {}

    record LegacyAccount(String accountId, long amount) {}

    private static final String ACCOUNT_ID = "account-1";
    private static final VersionedType ACCOUNT = VersionedType.of("account", "2.0.0");
    private static final VersionedType ACCOUNT_V1 = VersionedType.of("account", "1.0.0");
    private static final QualifiedName AGGREGATE_PAYLOAD_TYPE = new QualifiedName("com.example.LegacyAccount");
    private static final Converter CONVERTER = new JacksonConverter();
    private static final String STATE_300 = "{\"accountId\":\"account-1\",\"amount\":300}";
    private static final String STATE_9000 = "{\"accountId\":\"account-1\",\"amount\":9000}";

    private final AggregateBasedSnapshotStore snapshotStore = new AggregateBasedSnapshotStore();
    private final StorageEngineBackedEventStore eventStore = new StorageEngineBackedEventStore(
            new SnapshotCapableEventStorageEngine(new InMemoryEventStorageEngine(), snapshotStore),
            new SimpleEventBus(),
            new AnnotationBasedTagResolver()
    );

    @BeforeEach
    void publishEvents() {
        // the snapshots below all reflect the first two deposits, and are taken at the position of the third
        publish(new Deposited(ACCOUNT_ID, 5), new Deposited(ACCOUNT_ID, 5), new Deposited(ACCOUNT_ID, 7));
    }

    @Nested
    class WithAnEarlierVersion {

        @Test
        void shouldLiftASnapshotOfAnEarlierVersionIntoTheCurrentType() {
            // given
            SnapshotTransformerChain chain = chainOf(SnapshotTransformation.pathFrom(ACCOUNT_V1)
                    .to(ACCOUNT.version())
                    .transform(AccountV1.class, v1 -> new Account(v1.id(), v1.cents() / 100))
                    .build());
            storeSnapshot(ACCOUNT_V1, new AccountV1(ACCOUNT_ID, 100_000));

            // when
            Account account = source(chain);

            // then
            assertThat(account.balance()).isEqualTo(1007);
        }

        @Test
        void shouldSourceInFullWhenTheMapperDeclines() {
            // given
            SnapshotTransformerChain chain = chainOf(SnapshotTransformation.pathFrom(ACCOUNT_V1)
                    .to(ACCOUNT.version())
                    .transform(AccountV1.class, v1 -> null)
                    .build());
            storeSnapshot(ACCOUNT_V1, new AccountV1(ACCOUNT_ID, 100_000));

            // when
            Account account = source(chain);

            // then
            assertThat(account.balance()).isEqualTo(17);
        }

        @Test
        void shouldSourceInFullWhenTheStoredStateCannotBeRead() {
            // given
            SnapshotTransformerChain chain = chainOf(SnapshotTransformation.pathFrom(ACCOUNT_V1)
                    .to(ACCOUNT.version())
                    .transform(AccountV1.class, v1 -> new Account(v1.id(), v1.cents() / 100))
                    .build());
            storeSnapshot(ACCOUNT_V1, json("not json"));

            // when
            Account account = source(chain);

            // then
            assertThat(account.balance()).isEqualTo(17);
        }

        @Test
        void shouldPreferASnapshotOfTheCurrentType() {
            // given
            SnapshotTransformerChain chain = chainOf(SnapshotTransformation.pathFrom(ACCOUNT_V1)
                    .to(ACCOUNT.version())
                    .transform(AccountV1.class, v1 -> new Account(v1.id(), v1.cents() / 100))
                    .build());
            storeSnapshot(ACCOUNT_V1, new AccountV1(ACCOUNT_ID, 100_000));
            storeSnapshot(ACCOUNT, new Account(ACCOUNT_ID, 2000));

            // when
            Account account = source(chain);

            // then
            assertThat(account.balance()).isEqualTo(2007);
        }
    }

    @Nested
    class WithAnAxonFramework4Snapshot {

        private final SnapshotTransformerChain chain = chainOf(
                aggregateBasedPath(VersionedType.of(AGGREGATE_PAYLOAD_TYPE, "0.0.0")),
                aggregateBasedPath(VersionedType.of(AGGREGATE_PAYLOAD_TYPE, "1"))
        );

        @Test
        void shouldLiftASnapshotWithoutARevision() {
            // given
            snapshotStore.storeAggregateBased(AGGREGATE_PAYLOAD_TYPE, null, STATE_300);

            // when
            Account account = source(chain);

            // then
            assertThat(account.balance()).isEqualTo(307);
        }

        @Test
        void shouldLiftASnapshotWithARevision() {
            // given
            snapshotStore.storeAggregateBased(AGGREGATE_PAYLOAD_TYPE, "1", STATE_300);

            // when
            Account account = source(chain);

            // then
            assertThat(account.balance()).isEqualTo(307);
        }

        @Test
        void shouldSourceInFullWhenTheSnapshotHasARevisionNoPathStartsFrom() {
            // given
            snapshotStore.storeAggregateBased(AGGREGATE_PAYLOAD_TYPE, "2", STATE_300);

            // when
            Account account = source(chain);

            // then
            assertThat(account.balance()).isEqualTo(17);
        }

        @Test
        void shouldSourceInFullWhenTheSnapshotIsOfAnotherPayloadType() {
            // given a snapshot of another aggregate type with the same identifier, found by the lookup by aggregate
            snapshotStore.storeAggregateBased(new QualifiedName("com.example.Customer"), null,
                                      STATE_300);

            // when
            Account account = source(chain);

            // then
            assertThat(account.balance()).isEqualTo(17);
        }

        @Test
        void shouldNotLookForASnapshotTheChainHasNoPathFrom() {
            // given
            snapshotStore.storeAggregateBased(AGGREGATE_PAYLOAD_TYPE, null, STATE_300);

            // when
            Account account = source(chainOf(SnapshotTransformation.pathFrom(ACCOUNT_V1)
                    .to(ACCOUNT.version())
                    .transform(AccountV1.class, v1 -> new Account(v1.id(), v1.cents() / 100))
                    .build()));

            // then
            assertThat(account.balance()).isEqualTo(17);
            assertThat(snapshotStore.aggregateBasedCandidates).containsOnlyNulls();
        }

        @Test
        void shouldContinueWithItsOwnSnapshotWhileNewerAggregateBasedSnapshotsAreTaken() {
            // given an entity that took its own snapshot after starting from an Axon Framework 4 one
            snapshotStore.storeAggregateBased(AGGREGATE_PAYLOAD_TYPE, null, STATE_300);
            Account first = source(chain, SnapshotPolicy.afterEvents(0));
            snapshotStore.storeAggregateBased(AGGREGATE_PAYLOAD_TYPE, null, STATE_9000);

            // when
            Account second = source(chain);

            // then
            assertThat(first.balance()).isEqualTo(307);
            assertThat(second.balance()).isEqualTo(307);
        }

        private static SnapshotTransformation aggregateBasedPath(VersionedType aggregateBasedSource) {
            return SnapshotTransformation.pathFromAggregateBased(aggregateBasedSource)
                                         .storedAs(CONVERTER)
                                         .to(ACCOUNT)
                                         .transform(LegacyAccount.class, a -> new Account(a.accountId(), a.amount()))
                                         .build();
        }
    }

    @Test
    void shouldLeaveSourcingWithoutASnapshotUntouched() {
        // given
        EventStore store = new SnapshotTransformingEventStore(eventStore, chainOf(), CONVERTER);
        UnitOfWork uow = UnitOfWorkTestUtils.aUnitOfWork();

        // when
        List<Object> payloads = uow.executeWithResult(context -> {
            MessageStream<? extends EventMessage> stream = store.transaction(context).source(
                    SourcingCondition.conditionFor(EventCriteria.havingTags(Tag.of("account", ACCOUNT_ID)))
            );

            return stream.reduce(new ArrayList<>(), (list, entry) -> {
                list.add(entry.message().payload());
                return list;
            });
        }).orTimeout(5, TimeUnit.SECONDS).join();

        // then
        assertThat(payloads).hasSize(3);
    }

    private static SnapshotTransformerChain chainOf(SnapshotTransformation... transformations) {
        SnapshotTransformerChain.Builder builder = SnapshotTransformerChain.builder();

        for (SnapshotTransformation transformation : transformations) {
            builder.register(transformation);
        }

        return builder.build();
    }

    private Account source(SnapshotTransformerChain chain) {
        return source(chain, SnapshotPolicy.afterEvents(100));
    }

    private Account source(SnapshotTransformerChain chain, SnapshotPolicy policy) {
        SnapshottingEntityLifecycleHandler<String, Account> handler = new SnapshottingEntityLifecycleHandler<>(
                new SnapshotTransformingEventStore(eventStore, chain, CONVERTER),
                (id, context) -> EventCriteria.havingTags(Tag.of("account", id)),
                new AnnotationBasedTagResolver(),
                new InitializingEntityEvolver<>(
                        (id, message, context) -> new Account(id, 0),
                        (entity, event, context) -> event.payload() instanceof Deposited deposited
                                ? new Account(entity.id(), entity.balance() + deposited.amount())
                                : entity
                ),
                policy,
                ACCOUNT,
                CONVERTER,
                Account.class,
                snapshotStore
        );

        return UnitOfWorkTestUtils.aUnitOfWork()
                                  .executeWithResult(context -> handler.source(ACCOUNT_ID, context))
                                  .orTimeout(5, TimeUnit.SECONDS)
                                  .join();
    }

    private void publish(Object... events) {
        List<EventMessage> messages = new ArrayList<>();

        for (Object event : events) {
            messages.add(new GenericEventMessage(new MessageType(event.getClass()), event));
        }

        eventStore.publish(null, messages).orTimeout(5, TimeUnit.SECONDS).join();
    }

    private void storeSnapshot(VersionedType type, Object state) {
        snapshotStore.store(
                type.qualifiedName(),
                ACCOUNT_ID,
                new Snapshot(GlobalIndexPositions.of(2), type.version(), state, Instant.now(), Map.of()),
                null
        ).orTimeout(5, TimeUnit.SECONDS).join();
    }

    private static byte[] json(String json) {
        return json.getBytes(StandardCharsets.UTF_8);
    }

    /*
     * A snapshot store that also holds aggregate-based snapshots, stored under their aggregate identifier alone, as
     * Axon Server does. Like Axon Server, it reports such a snapshot under its own payload type and revision whenever a
     * aggregate-based lookup is requested, leaving it to the caller to check that it is the one asked for.
     */
    private static final class AggregateBasedSnapshotStore implements SnapshotStore {

        private final InMemorySnapshotStore delegate = new InMemorySnapshotStore();
        private final Map<Object, SnapshotMatch> aggregateBasedSnapshots = new ConcurrentHashMap<>();
        private final List<@Nullable SnapshotCandidate> aggregateBasedCandidates = new ArrayList<>();

        void storeAggregateBased(QualifiedName payloadType, @Nullable String revision, String json) {
            Snapshot snapshot = new Snapshot(
                    GlobalIndexPositions.of(2), revision == null ? "0.0.0" : revision, json(json), Instant.now(),
                    Map.of()
            );

            aggregateBasedSnapshots.put(ACCOUNT_ID, new SnapshotMatch(payloadType, snapshot));
        }

        @Override
        public CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot,
                                             @Nullable ProcessingContext context) {
            return delegate.store(qualifiedName, identifier, snapshot, context);
        }

        @Override
        public CompletableFuture<@Nullable SnapshotMatch> load(List<SnapshotCandidate> candidates,
                                                               @Nullable SnapshotCandidate aggregateBasedCandidate,
                                                               Object identifier,
                                                               @Nullable ProcessingContext context) {
            aggregateBasedCandidates.add(aggregateBasedCandidate);

            return delegate.load(candidates, identifier, context).thenApply(
                    match -> match != null || aggregateBasedCandidate == null
                            ? match
                            : aggregateBasedSnapshots.get(identifier)
            );
        }

        @Override
        public CompletableFuture<@Nullable SnapshotMatch> load(List<SnapshotCandidate> candidates, Object identifier,
                                                               @Nullable ProcessingContext context) {
            return load(candidates, null, identifier, context);
        }

        // temporary: Axon Framework keeps this deprecated method until Axoniq Framework has migrated
        @Override
        @SuppressWarnings("removal")
        public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier,
                                                          @Nullable ProcessingContext context) {
            return delegate.load(qualifiedName, identifier, context);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
        }
    }
}
