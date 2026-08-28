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

package io.axoniq.framework.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.common.jdbc.ConnectionProvider;
import org.axonframework.conversion.CachingSupplier;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStoreTestSuite;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

public class PostgresStorageEngineBackedEventStoreIT extends StorageEngineBackedEventStoreTestSuite<PostgresqlEventStorageEngine> {
    private static final UnitOfWorkFactory FACTORY = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE);

    private static PostgreSQLContainer postgresContainer;
    private static DataSource dataSource;

    private static PostgresqlEventStorageEngine engine;
    private static EventConverter eventConverter;

    @SuppressWarnings("resource")
    @BeforeAll
    static void buildEngine() {
        postgresContainer = new PostgreSQLContainer("postgres:16.2")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");

        postgresContainer.start();

        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(postgresContainer.getJdbcUrl());
        config.setUsername(postgresContainer.getUsername());
        config.setPassword(postgresContainer.getPassword());

        // Sized well above MultiTagLockOrdering's largest concurrent transaction count, so those
        // tests get genuine concurrency instead of some transactions queuing for a connection.
        config.setMaximumPoolSize(25);
        config.setMinimumIdle(1);
        config.setAutoCommit(false);

        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void stopContainer() {
        if (engine != null) {
            engine.close();
            // getStorageEngine() lazily caches this in a static field; without resetting it here, a
            // rerun's fresh dataSource (recreated below) would be ignored in favor of this stale
            // engine, which is still bound to the pool we're about to close.
            engine = null;
        }

        if (dataSource instanceof HikariDataSource hikariDataSource) {
            hikariDataSource.close();
        }

        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }

    /**
     * Covers the edge cases around type-based append conditions: tag/type hash
     * collisions must not crash the batched lock upsert, unrelated tags sharing a hash
     * bucket must not cause a false conflict, and events that are committed but not yet
     * finalized (still carrying a temporary negative {@code global_index}) must still fully
     * participate in conflict detection - both when they genuinely conflict and when they
     * don't.
     */
    @Nested
    class TypeAndHashConflictDetection {

        /**
         * Two distinct tags known to collide under {@link MurmurHash3#hash32} masked to the
         * engine's current {@link PostgresqlEventStorageEngine#hashCapacity} - found by brute
         * force against the exact algorithm and capacity used by the engine. Only valid as
         * long as both stay unchanged, which {@link #hashCapacityAssumptionStillHolds()}
         * guards.
         */
        private static final Tag COLLIDING_TAG_A1 = new Tag("k1", "v1015");
        private static final Tag COLLIDING_TAG_A2 = new Tag("k1", "v1288");

        /**
         * A second, disjoint colliding pair - kept separate from {@link #COLLIDING_TAG_A1}/
         * {@link #COLLIDING_TAG_A2} so the two hash-collision tests below don't interfere with
         * each other in this suite's long-lived, never-reset store.
         */
        private static final Tag COLLIDING_TAG_B1 = new Tag("courseId", "val-766");
        private static final Tag COLLIDING_TAG_B2 = new Tag("courseId", "val-874");

        @Test
        void hashCapacityAssumptionStillHolds() {
            assertThat(engine.hashCapacity)
                .as("the hardcoded hash-collision fixtures in this test are only valid for this exact capacity - "
                    + "regenerate them (see the brute-force finder used to derive them) if this ever changes")
                .isEqualTo(1024 * 1024);
        }

        @Test
        void collidingTagsWithinOneCriterionDoNotCrash() {
            assertThat(maskedHash(COLLIDING_TAG_A1)).isEqualTo(maskedHash(COLLIDING_TAG_A2));

            UnitOfWork uow = unitOfWork();

            uow.runOnInvocation(pc -> {
                EventStoreTransaction tx = eventStore.transaction(pc);
                EventCriteria criteria = EventCriteria.havingTags(COLLIDING_TAG_A1, COLLIDING_TAG_A2);

                tx.overrideAppendCondition(condition -> AppendCondition.withCriteria(criteria));
                tx.appendEvent(message(new HashConflictTestEvent(TAG1.value())));
            });

            assertThatCode(() -> execute(uow)).doesNotThrowAnyException();
        }

        @Test
        void collidingButUnrelatedTagsDoNotCauseFalseConflict() {
            assertThat(maskedHash(COLLIDING_TAG_B1)).isEqualTo(maskedHash(COLLIDING_TAG_B2));

            UnitOfWork firstAppend = unitOfWork();

            firstAppend.runOnInvocation(pc -> {
                EventStoreTransaction tx = eventStore.transaction(pc);
                EventCriteria criteria = EventCriteria.havingTags(COLLIDING_TAG_B1);

                tx.overrideAppendCondition(condition -> AppendCondition.withCriteria(criteria));
                tx.appendEvent(message(new HashConflictTestEvent(TAG1.value())));
            });

            execute(firstAppend);

            // second append only cares about the OTHER colliding tag - unrelated to the first
            UnitOfWork secondAppend = unitOfWork();

            secondAppend.runOnInvocation(pc -> {
                EventStoreTransaction tx = eventStore.transaction(pc);
                EventCriteria criteria = EventCriteria.havingTags(COLLIDING_TAG_B2);

                tx.overrideAppendCondition(condition -> AppendCondition.withCriteria(criteria));
                tx.appendEvent(message(new HashConflictTestEvent(TAG2.value())));
            });

            assertThatCode(() -> execute(secondAppend)).doesNotThrowAnyException();
        }

        @Test
        void conflictAgainstUnfinalizedEventIsStillDetected() throws SQLException {
            Tag tag = new Tag("unfinalizedConflict", UUID.randomUUID().toString());
            String typeName = "test.UnfinalizedConflictingType";

            seedUnfinalizedEvent(-1_000_000_101L, tag, typeName);

            UnitOfWork uow = unitOfWork();

            uow.runOnInvocation(pc -> {
                EventStoreTransaction tx = eventStore.transaction(pc);
                EventCriteria criteria = EventCriteria.havingTags(tag).andBeingOneOfTypes(typeName);

                tx.overrideAppendCondition(condition -> AppendCondition.withCriteria(criteria));
                tx.appendEvent(message(new HashConflictTestEvent(TAG1.value())));
            });

            assertThatThrownBy(() -> execute(uow))
                .isInstanceOf(AppendEventsTransactionRejectedException.class);
        }

        @Test
        void nonMatchingUnfinalizedEventDoesNotCauseFalseConflict() throws SQLException {
            Tag tag = new Tag("unfinalizedNoConflict", UUID.randomUUID().toString());
            String seededType = "test.UnfinalizedSeededType";
            String narrowedType = "test.UnfinalizedNarrowedType";  // deliberately different from seededType

            seedUnfinalizedEvent(-1_000_000_102L, tag, seededType);

            UnitOfWork uow = unitOfWork();

            uow.runOnInvocation(pc -> {
                EventStoreTransaction tx = eventStore.transaction(pc);
                EventCriteria criteria = EventCriteria.havingTags(tag).andBeingOneOfTypes(narrowedType);

                tx.overrideAppendCondition(condition -> AppendCondition.withCriteria(criteria));
                tx.appendEvent(message(new HashConflictTestEvent(TAG1.value())));
            });

            assertThatCode(() -> execute(uow)).doesNotThrowAnyException();
        }

        /**
         * Directly seeds an event that is committed but never finalized - it keeps the
         * temporary negative {@code global_index} it would have had right after insertion,
         * forever, since nothing here ever runs {@link PostgresqlFinalizer}. This lets tests
         * deterministically construct the "committed but not yet renumbered" state without
         * racing the real (asynchronous) finalizer.
         * <p>
         * Replicates by hand what {@code internalAppendEvents} does for a real append: the
         * {@code axon_events_write_type_tag} trigger fires automatically on the raw {@code
         * events} insert (writing the reserved type tag row), but the {@code consistency_tags}
         * bookkeeping - one row for the plain tag, one for the reserved type - has to be
         * written explicitly here, since that logic lives in Java, not in a trigger.
         *
         * @param negativeGlobalIndex the temporary index to seed with, must be negative and
         *                            not otherwise in use
         * @param tag the event's own (single) tag
         * @param typeName the event's type, as a plain qualified name string
         * @return the (randomly generated) identifier the seeded event was given, so callers
         *         can later look up its real, finalized global index
         */
        private String seedUnfinalizedEvent(long negativeGlobalIndex, Tag tag, String typeName) throws SQLException {
            String identifier = UUID.randomUUID().toString();

            try (Connection connection = dataSource.getConnection()) {
                try (PreparedStatement eventInsert = connection.prepareStatement(
                    "INSERT INTO events (global_index, timestamp, payload, metadata, identifier, type, type_version) "
                        + "VALUES (?, ?, ?, ?::json, ?, ?, ?)"
                )) {
                    eventInsert.setLong(1, negativeGlobalIndex);
                    eventInsert.setTimestamp(2, Timestamp.from(baseTime));
                    eventInsert.setBytes(3, new byte[0]);
                    eventInsert.setString(4, "{}");
                    eventInsert.setString(5, identifier);
                    eventInsert.setString(6, typeName);
                    eventInsert.setString(7, "0.0.1");
                    eventInsert.execute();
                }

                try (PreparedStatement tagInsert = connection.prepareStatement(
                    "INSERT INTO tags (global_index, key, value) VALUES (?, ?, ?)"
                )) {
                    tagInsert.setLong(1, negativeGlobalIndex);
                    tagInsert.setString(2, tag.key());
                    tagInsert.setString(3, tag.value());
                    tagInsert.execute();
                }

                try (PreparedStatement consistencyUpsert = connection.prepareStatement(
                    "INSERT INTO consistency_tags (tag_hash, global_index) VALUES (?, ?) "
                        + "ON CONFLICT (tag_hash) DO UPDATE SET global_index = EXCLUDED.global_index"
                )) {
                    consistencyUpsert.setInt(1, maskedHash(tag));
                    consistencyUpsert.setLong(2, negativeGlobalIndex);
                    consistencyUpsert.execute();

                    consistencyUpsert.setInt(1, maskedHash(new Tag(PostgresqlEventStorageEngine.TYPE_TAG_KEY, typeName)));
                    consistencyUpsert.setLong(2, negativeGlobalIndex);
                    consistencyUpsert.execute();
                }

                connection.commit();
            }

            return identifier;
        }

        private int maskedHash(Tag tag) {
            return MurmurHash3.hash32((tag.key() + ":" + tag.value()).getBytes(StandardCharsets.UTF_8)) & engine.hashMask;
        }

        /**
         * Looks up the real, finalized {@code global_index} of an event by its identifier -
         * used to read back the position a {@link #seedUnfinalizedEvent} row ends up at once
         * {@link PostgresqlFinalizer} has renumbered it.
         */
        private long globalIndexOf(String identifier) throws SQLException {
            try (
                Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement("SELECT global_index FROM events WHERE identifier = ?");
            ) {
                ps.setString(1, identifier);

                try (ResultSet resultSet = ps.executeQuery()) {
                    assertThat(resultSet.next()).as("event with identifier " + identifier + " should exist").isTrue();

                    return resultSet.getLong(1);
                }
            }
        }

        /**
         * Reads the current {@code global_index} stored in {@code consistency_tags} for a given
         * attribute hash - the watermark {@link PostgresqlEventStorageEngine#lock} consults for
         * future append conditions touching that hash.
         */
        private long consistencyTagWatermark(int tagHash) throws SQLException {
            try (
                Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement("SELECT global_index FROM consistency_tags WHERE tag_hash = ?");
            ) {
                ps.setInt(1, tagHash);

                try (ResultSet resultSet = ps.executeQuery()) {
                    assertThat(resultSet.next()).as("consistency_tags row for hash " + tagHash + " should exist").isTrue();

                    return resultSet.getLong(1);
                }
            }
        }

        /**
         * Proves that {@code LEAST(...)} in {@link PostgresqlEventStorageEngine#CONSISTENCY_TAGS_LOCK}'s
         * fallback is required: without it, a stale write can regress a hash's consistency watermark
         * backwards, silently forgetting that a more recent (still-unfinalized) event already touched it.
         * <p>
         * Two distinct tags that hash-collide (found by brute force, like {@link #COLLIDING_TAG_A1}/
         * {@link #COLLIDING_TAG_A2}) share one {@code consistency_tags} row. A phantom, still-unfinalized
         * event is seeded directly using one of them at a very negative {@code global_index} - simulating
         * a concurrent transaction that claimed this hash bucket, but has not committed/finalized yet. A
         * real append then targets the *other* colliding tag: the fast check fails (the existing value is
         * negative), so the fallback runs; since the phantom's real tag does not actually match this
         * append's criterion, {@code NOT EXISTS} finds no conflict and the row is updated with {@code
         * LEAST(existing, EXCLUDED)} - where {@code EXCLUDED} is the real event's own temporary index,
         * a small negative number nowhere near as negative as the phantom's deliberately huge seeded
         * value, so {@code LEAST} must keep the existing (phantom) value rather than the new one.
         * <p>
         * Both events are finalized together (as one batch) once the append commits. If the update kept
         * the phantom's value (the correct, {@code LEAST}-guarded behavior), the watermark is later
         * renumbered to the phantom's own real, higher (later) position - since {@code
         * PostgresqlFinalizer} always finalizes more-negative (later-inserted) rows to higher positions.
         * If the update instead overwrote it with the real event's value (the bug, without {@code LEAST}),
         * the watermark ends up renumbered to the real event's own, lower, position instead - permanently
         * losing all record of the phantom's later touch on this hash.
         */
        @Test
        void leastPreventsWatermarkRegressionFromAStaleConcurrentWrite() throws SQLException {
            Tag phantomTag = new Tag("leastTest", "v257");
            Tag colliderTag = new Tag("leastTest", "v2727");

            assertThat(maskedHash(phantomTag))
                .as("fixture assumption: these two tags must hash-collide for this test to be meaningful")
                .isEqualTo(maskedHash(colliderTag));

            String phantomIdentifier = seedUnfinalizedEvent(-1_000_000_900L, phantomTag, "test.LeastWatermarkPhantomType");

            UnitOfWork uow = unitOfWork();

            uow.runOnInvocation(pc -> {
                EventStoreTransaction tx = eventStore.transaction(pc);
                EventCriteria criteria = EventCriteria.havingTags(colliderTag);

                tx.overrideAppendCondition(condition -> AppendCondition.withCriteria(criteria));
                tx.appendEvent(message(new HashConflictTestEvent(TAG1.value())));
            });

            // no conflict expected: the phantom's real tag doesn't match colliderTag, only its hash
            assertThatCode(() -> execute(uow)).doesNotThrowAnyException();

            long phantomFinalPosition = globalIndexOf(phantomIdentifier);
            long watermark = consistencyTagWatermark(maskedHash(phantomTag));

            assertThat(watermark)
                .as("the watermark must still reflect the phantom's (later) touch on this hash, "
                    + "not have regressed to the newly appended event's own (earlier) position")
                .isEqualTo(phantomFinalPosition);
        }

        @Event
        record HashConflictTestEvent(@EventTag(key = "Course") String id) {}
    }

    /**
     * Verifies that concurrent transactions locking overlapping sets of tags never deadlock,
     * regardless of the order {@code EventCriterion.tags()} happens to iterate its tags in - it's
     * backed by {@code Set.copyOf(...)}, whose iteration order is randomized per JVM run
     * specifically to discourage code from relying on it. Twenty tags per criterion makes it
     * overwhelmingly likely that at least one of the many possible iteration orders would
     * produce a genuine ABBA deadlock if the engine's hash-lock ordering were ever changed away
     * from a consistent, global order (e.g. reverting the {@code TreeSet} used to build {@code
     * hashesToLock} back to something order-preserving instead).
     * <p>
     * This is deliberately a real-behavior regression guard rather than a white-box check of the
     * ordering itself, so a sporadic false negative - an unlucky iteration order that happens not
     * to trigger a deadlock even without the fix - is an accepted trade-off. Each test therefore
     * runs as three independent repetitions (fresh tags each time): even if any single repetition
     * only has a moderate chance of hitting a genuine race, requiring all three to miss it in a
     * row compounds that chance down considerably.
     */
    @Nested
    class MultiTagLockOrdering {

        /**
         * A dedicated pool, sized comfortably above the largest concurrent transaction count used
         * below, so every transaction actually races the others.
         */
        private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(30);

        @AfterAll
        static void afterAll() {
            EXECUTOR.shutdown();
        }

        @RepeatedTest(3)
        void manyOverlappingTagsWhereOneIsASubsetOfTheOtherDoNotDeadlock() {
            List<Tag> tags = IntStream.range(0, 20)
                .mapToObj(i -> new Tag("lockOrderTest", UUID.randomUUID().toString()))
                .toList();

            EventCriteria allTags = EventCriteria.havingTags(tags.toArray(new Tag[0]));
            EventCriteria nineteenTags = EventCriteria.havingTags(tags.subList(1, tags.size()).toArray(new Tag[0]));

            assertNoDeadlock(List.of(allTags, nineteenTags));
        }

        @RepeatedTest(3)
        void manyOverlappingTagsWithPartialOverlapDoNotDeadlock() {
            List<Tag> tags = IntStream.range(0, 20)
                .mapToObj(i -> new Tag("lockOrderTest", UUID.randomUUID().toString()))
                .toList();

            EventCriteria firstSixteen = EventCriteria.havingTags(tags.subList(0, 16).toArray(new Tag[0]));
            EventCriteria lastSixteen = EventCriteria.havingTags(tags.subList(4, 20).toArray(new Tag[0]));

            assertNoDeadlock(List.of(firstSixteen, lastSixteen));
        }

        /**
         * A larger, many-way version of the same idea: twenty concurrent transactions, each
         * narrowed to a random 14-tag subset of a shared 24-tag pool. More participants racing
         * over more overlapping pairs gives many more chances for some pair to genuinely collide
         * mid-statement than a single pair does, making this the most effective of the three at
         * catching a reverted (order-preserving) {@code hashesToLock}.
         */
        @RepeatedTest(3)
        void manyConcurrentTransactionsWithRandomOverlapDoNotDeadlock() {
            List<Tag> pool = IntStream.range(0, 24)
                .mapToObj(i -> new Tag("lockOrderTest", UUID.randomUUID().toString()))
                .toList();

            Random random = new Random();
            List<EventCriteria> criteria = IntStream.range(0, 20)
                .<EventCriteria>mapToObj(i -> {
                    List<Tag> shuffled = new ArrayList<>(pool);

                    Collections.shuffle(shuffled, random);

                    return EventCriteria.havingTags(shuffled.subList(0, 14).toArray(new Tag[0]));
                })
                .toList();

            assertNoDeadlock(criteria);
        }

        /**
         * Runs one transaction per given criterion, all concurrently, each narrowed (via {@link
         * EventStoreTransaction#overrideAppendCondition}) to its own criterion, and asserts all of
         * them complete within a generous timeout - a real ABBA deadlock would surface as a
         * {@code deadlock_detected} exception well within it (Postgres's own deadlock detector
         * fires within about a second by default), not a silent hang.
         *
         * @param criteria one append condition per concurrent transaction, cannot be {@code null} or empty
         */
        private void assertNoDeadlock(List<EventCriteria> criteria) {
            CountDownLatch ready = new CountDownLatch(criteria.size());
            CountDownLatch go = new CountDownLatch(1);

            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (EventCriteria criterion : criteria) {
                UnitOfWork uow = unitOfWork();

                uow.runOnInvocation(pc -> {
                    EventStoreTransaction tx = eventStore.transaction(pc);

                    tx.overrideAppendCondition(condition -> AppendCondition.withCriteria(criterion));
                    ready.countDown();
                    awaitLatch(go);
                    tx.appendEvent(message(new LockOrderTestEvent(UUID.randomUUID().toString())));
                });

                futures.add(CompletableFuture.runAsync(() -> execute(uow), EXECUTOR));
            }

            awaitLatch(ready);
            go.countDown();

            assertThatCode(() -> CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(15, TimeUnit.SECONDS))
                .doesNotThrowAnyException();
        }

        @Event
        record LockOrderTestEvent(@EventTag(key = "Course") String id) {}
    }

    /**
     * Verifies {@link SourcingStrategy.Snapshot} sourcing end to end, through {@link
     * EventStoreTransaction#source(SourcingCondition)} rather than the engine directly - this
     * exercises the full single-round-trip snapshot query in {@link
     * PostgresqlEventStorageEngine#source(SourcingCondition)}, including the leading {@link
     * SnapshotEventMessage} and the pagination continuation for more events than fit in one
     * internal page (50).
     */
    @Nested
    class SnapshotSourcing {

        private QualifiedName qualifiedName;
        private String identifier;

        @BeforeEach
        void setUp() {
            qualifiedName = new QualifiedName(SnapshotSourcing.class);
            identifier = UUID.randomUUID().toString();
        }

        @Test
        void sourcingWithOnlyASnapshotReturnsJustTheSnapshot() {
            Snapshot snapshot = storeSnapshot(0);

            SnapshotSourcingResult result = sourceWithSnapshot();

            assertSnapshotMatches(result.snapshot(), snapshot);
            assertThat(result.events()).isEmpty();
        }

        @Test
        void sourcingWithASnapshotAndAFewEventsReturnsBoth() {
            Snapshot snapshot = storeSnapshot(currentPosition());

            EventMessage event1 = message(new SnapshotTestEvent(TAG1.value(), "1"));
            EventMessage event2 = message(new SnapshotTestEvent(TAG1.value(), "2"));
            EventMessage event3 = message(new SnapshotTestEvent(TAG1.value(), "3"));

            unitOfWork().executeWithResult(pc -> eventStore.publish(pc, event1, event2, event3)).join();

            SnapshotSourcingResult result = sourceWithSnapshot();

            assertSnapshotMatches(result.snapshot(), snapshot);
            assertThat(result.events())
                .usingComparatorForType(EVENT_COMPARATOR, GenericEventMessage.class)
                .containsExactly(event1, event2, event3);
        }

        @Test
        void sourcingWithASnapshotAndManyEventsContinuesBeyondFirstInternalPage() {
            Snapshot snapshot = storeSnapshot(currentPosition());

            // The engine internally pages in batches of 50 - use more than that to verify the
            // continuation after the initial snapshot-aware batch picks up every remaining
            // event, with no gaps or duplicates.
            EventMessage[] events = new EventMessage[550];

            for (int i = 0; i < events.length; i++) {
                events[i] = message(new SnapshotTestEvent(TAG1.value(), "event-" + i));
            }

            unitOfWork().executeWithResult(pc -> eventStore.publish(pc, events)).join();

            SnapshotSourcingResult result = sourceWithSnapshot();

            assertSnapshotMatches(result.snapshot(), snapshot);
            assertThat(result.events())
                .usingComparatorForType(EVENT_COMPARATOR, GenericEventMessage.class)
                .containsExactly(events);
        }

        private long currentPosition() {
            return unitOfWork().executeWithResult(eventStore::latestToken).join().position().orElse(0);
        }

        private Snapshot storeSnapshot(long position) {
            Snapshot snapshot = new Snapshot(new GlobalIndexPosition(position), "0.0.1", "snapshot-payload", Instant.now(), Map.of());

            engine.store(qualifiedName, identifier, snapshot, null).join();

            return snapshot;
        }

        /**
         * Compares {@code actual} against {@code expected} field by field rather than via {@code
         * equals()}, since two things never round-trip byte-for-byte through Postgres: the engine
         * always returns the snapshot payload as the raw, still-serialized {@code byte[]} (payload
         * deserialization is the caller's responsibility, not the engine's - see {@code
         * SnapshottingEntityLifecycleHandler#convertSnapshotPayload}), and {@code TIMESTAMPTZ} only
         * has microsecond precision, truncating part of {@link Instant#now()}'s nanoseconds.
         */
        private void assertSnapshotMatches(@Nullable Snapshot actual, Snapshot expected) {
            assertThat(actual).isNotNull();
            assertThat(actual.position()).isEqualTo(expected.position());
            assertThat(actual.version()).isEqualTo(expected.version());
            assertThat(actual.metadata()).isEqualTo(expected.metadata());
            assertThat(actual.timestamp()).isCloseTo(expected.timestamp(), within(1, ChronoUnit.MILLIS));
            assertThat(eventConverter.convert(actual.payload(), String.class)).isEqualTo(expected.payload());
        }

        /**
         * Sources with the {@link SourcingStrategy.Snapshot} strategy and splits the result into
         * the leading snapshot (if any) and the tail events, since the generic {@code source(...)}
         * helper on the base suite assumes every entry converts to {@code CourseUpdated}, which a
         * leading {@link SnapshotEventMessage} does not.
         */
        private SnapshotSourcingResult sourceWithSnapshot() {
            SourcingCondition condition = SourcingCondition.conditionFor(
                new SourcingStrategy.Snapshot(qualifiedName, identifier, null),
                EventCriteria.havingTags(TAG1)
            );

            return unitOfWork().executeWithResult(pc -> {
                EventStoreTransaction tx = eventStore.transaction(pc);
                MessageStream<? extends EventMessage> stream = tx.source(condition);
                AtomicReference<Snapshot> snapshotRef = new AtomicReference<>();

                return stream.reduce(new ArrayList<EventMessage>(), (list, entry) -> {
                    EventMessage message = entry.message();

                    if (message instanceof SnapshotEventMessage snapshotEventMessage) {
                        snapshotRef.set(snapshotEventMessage.payload());
                    }
                    else {
                        list.add(message.withConvertedPayload(SnapshotTestEvent.class, eventConverter));
                    }

                    return list;
                }).thenApply(events -> new SnapshotSourcingResult(snapshotRef.get(), events));
            }).join();
        }

        private record SnapshotSourcingResult(@Nullable Snapshot snapshot, List<EventMessage> events) {}

        @Event
        record SnapshotTestEvent(@EventTag(key = "Course") String id, String data) {}
    }

    /**
     * Regression test for a race in {@link PostgresqlEventStorageEngine#EVENTS_READ_MULTIPLE}: its
     * trailing {@code SELECT MAX(global_index)} watermark statement runs as its own, separately
     * snapshotted {@code READ COMMITTED} statement, immediately after the page-read statement rather
     * than as part of it. If {@link PostgresqlFinalizer} finalizes new events in the gap between the
     * two, that watermark can see them even though the page-read never did. {@link
     * PostgresqlEventStorageEngine#stream} advances its cursor straight past whatever the watermark
     * reports - any event caught in that gap is skipped for good.
     * <p>
     * Reproduced here by continuously appending events one commit at a time - so the asynchronous
     * finalizer is always racing a fresh commit - while a single long-lived tracking stream consumes
     * them concurrently. On buggy code, some events never arrive, no matter how long the test waits.
     */
    @Nested
    class StreamingUnderConcurrentAppends {

        private static final int EVENT_COUNT = 400;

        @Test
        @Timeout(value = 5, unit = TimeUnit.MINUTES)
        void streamMustDeliverEveryAppendedEventDespiteConcurrentFinalization() throws InterruptedException {
            Tag tag = new Tag("Course", UUID.randomUUID().toString());
            EventCriteria criteria = EventCriteria.havingTags(tag);
            TrackingToken token = unitOfWork().executeWithResult(eventStore::latestToken).join();

            Set<String> delivered = Collections.synchronizedSet(new LinkedHashSet<>());
            AtomicBoolean running = new AtomicBoolean(true);
            MessageStream<EventMessage> stream = eventStore.open(StreamingCondition.conditionFor(token, criteria), null);

            Thread consumer = Thread.ofPlatform().name("stream-skip-consumer").start(() -> {
                while (running.get()) {
                    Optional<MessageStream.Entry<EventMessage>> entry = stream.next();

                    if (entry.isEmpty()) {
                        try {
                            Thread.sleep(1);
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        continue;
                    }

                    EventMessage converted = entry.get().message().withConvertedPayload(StreamSkipProbe.class, eventConverter);

                    delivered.add(converted.payloadAs(StreamSkipProbe.class).seq());
                }
            });

            List<String> appended = new ArrayList<>();

            try {
                for (int i = 0; i < EVENT_COUNT; i++) {
                    String seq = "e" + i;

                    appended.add(seq);
                    appendProbe(tag, seq);
                }

                List<String> missing = missingAfterSettling(appended, delivered);

                assertThat(missing)
                    .as("every appended event must be delivered by the stream, %d of %d were skipped",
                        missing.size(), EVENT_COUNT)
                    .isEmpty();
            }
            finally {
                running.set(false);
                consumer.interrupt();
                consumer.join(TimeUnit.SECONDS.toMillis(10));
                stream.close();
            }
        }

        private void appendProbe(Tag tag, String seq) {
            UnitOfWork uow = unitOfWork();

            uow.runOnInvocation(pc -> eventStore.transaction(pc).appendEvent(message(new StreamSkipProbe(tag.value(), seq))));

            execute(uow);
        }

        /**
         * Returns the events the consumer never received, once it has stopped receiving anything new.
         * A skipped event is gone for good, so waiting longer cannot recover it - this only has to
         * outlast normal delivery.
         */
        private List<String> missingAfterSettling(List<String> appended, Set<String> delivered) throws InterruptedException {
            int previous = -1;

            for (int quietRounds = 0; quietRounds < 5; ) {
                Thread.sleep(500);

                int current = delivered.size();

                quietRounds = current == previous ? quietRounds + 1 : 0;
                previous = current;
            }

            synchronized (delivered) {
                return appended.stream().filter(seq -> !delivered.contains(seq)).toList();
            }
        }

        @Event
        record StreamSkipProbe(@EventTag(key = "Course") String id, String seq) {}
    }

    @Override
    protected PostgresqlEventStorageEngine getStorageEngine(EventConverter converter) {
        eventConverter = converter;

        if (engine == null) {
            engine = new PostgresqlEventStorageEngine(dataSource, converter);
        }

        return engine;
    }

    @Override
    protected UnitOfWork unitOfWork() {
        UnitOfWork unitOfWork = FACTORY.create();

        var cp = new ConnectionProvider() {
            private Connection obtainedConnection;

            @Override
            public synchronized Connection getConnection() throws SQLException {
                if (obtainedConnection == null) {
                    obtainedConnection = dataSource.getConnection();

                    obtainedConnection.setAutoCommit(false);
                }

                return obtainedConnection;
            }

            synchronized void commit() throws SQLException {
                if (obtainedConnection != null) {
                    obtainedConnection.commit();
                }
            }

            synchronized void rollback() throws SQLException {
                if (obtainedConnection != null) {
                    obtainedConnection.rollback();
                }
            }

            synchronized void close() {
                try {
                    if (obtainedConnection != null) {
                        obtainedConnection.close();
                    }
                }
                catch (SQLException e) {
                    // Ignore, if closing fails, the system is likely in a bad state already
                }
            }
        };

        unitOfWork.runOnPreInvocation(pc -> {
            pc.putResource(
                JdbcTransactionalExecutorProvider.SUPPLIER_KEY,
                CachingSupplier.of(() -> new ConnectionExecutor(cp))
            );

            pc.onCommit(p -> {
                try {
                    cp.commit();

                    return CompletableFuture.completedFuture(null);
                }
                catch (SQLException e) {
                    return CompletableFuture.failedFuture(e);
                }
                finally {
                    cp.close();
                }
            });

            pc.onError((p, phase, e) -> {
                try {
                    cp.rollback();
                }
                catch (SQLException se) {
                    e.addSuppressed(se);
                }
                finally {
                    cp.close();
                }
            });
        });

        return unitOfWork;
    }

}
