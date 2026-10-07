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

package io.axoniq.framework.messaging.eventstreaming.checkpoint;

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.common.jdbc.ConnectionProvider;
import org.axonframework.conversion.CachingSupplier;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.core.unitofwork.transaction.Transaction;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.GenericTokenTableFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStoreConfiguration;
import org.hsqldb.jdbc.JDBCDataSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests that a node keeps its segment claim while a slow participant delays the checkpoint, so a second node on the
 * same token store does not take over the segment and handle its events again.
 */
class ClaimExpiryDuringCheckpointWaitTest {

    private static final Logger logger = LoggerFactory.getLogger(ClaimExpiryDuringCheckpointWaitTest.class);
    private static final String PROCESSOR_NAME = "claim-expiry";
    private static final Duration CLAIM_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration SLOW_CHECKPOINT = CLAIM_TIMEOUT.multipliedBy(3);
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    private final AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource(false, false);
    private final JDBCDataSource dataSource = new JDBCDataSource();
    private final List<AxonConfiguration> nodes = new ArrayList<>();
    private final long begin = System.nanoTime();

    @BeforeEach
    void setUp() {
        dataSource.setUrl("jdbc:hsqldb:mem:claim-expiry-" + UUID.randomUUID() + ";hsqldb.tx=mvcc");
        dataSource.setUser("sa");
        dataSource.setPassword("");
        jdbcTokenStore("schema").createSchema(GenericTokenTableFactory.INSTANCE);
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(AxonConfiguration::shutdown);
    }

    @Nested
    class CoordinatorClaimExtensionOff {

        @Test
        void secondNodeDoesNotTakeOverTheSegmentWhileTheFirstWaitsForItsParticipant() {
            // given
            Node first = startNode("node-1", false);
            Node second = secondNodeAfterTheFirstClaimed(first);

            // when
            Contest contest = contest(first, second);

            // then
            assertThat(second.participant.handled).as(contest.summary()).doesNotContain("contested");
            assertThat(first.store.storeFailures).as(contest.summary()).isEmpty();
        }
    }

    @Nested
    class CoordinatorClaimExtensionOn {

        @Test
        void secondNodeDoesNotTakeOverTheSegmentWhileTheFirstWaitsForItsParticipant() {
            // given
            Node first = startNode("node-1", true);
            Node second = secondNodeAfterTheFirstClaimed(first);

            // when
            Contest contest = contest(first, second);

            // then
            assertThat(second.participant.handled).as(contest.summary()).doesNotContain("contested");
            assertThat(first.store.storeFailures).as(contest.summary()).isEmpty();
        }
    }

    private Node secondNodeAfterTheFirstClaimed(Node first) {
        eventSource.publishMessage(EventTestUtils.asEventMessage("warmup"));
        await().atMost(TIMEOUT).until(() -> first.participant.handled.contains("warmup")
                && first.store.stores.get() > 0);
        Node second = startNode("node-2", false);
        await().pollDelay(CLAIM_TIMEOUT.multipliedBy(2)).atMost(TIMEOUT).until(() -> true);
        assertThat(second.store.claimedAt.get()).as("second node claimed while the first was idle").isZero();
        return second;
    }

    private Contest contest(Node first, Node second) {
        first.participant.slowOnce.set(true);
        eventSource.publishMessage(EventTestUtils.asEventMessage("contested"));
        await().atMost(TIMEOUT).until(() -> first.participant.slowCompletedAt.get() > 0);
        await().pollDelay(Duration.ofSeconds(2)).atMost(TIMEOUT).until(() -> true);
        Contest contest = new Contest(first, second);
        logger.warn("{}", contest.summary());
        return contest;
    }

    private Node startNode(String nodeId, boolean coordinatorExtendsClaims) {
        RecordingTokenStore store = new RecordingTokenStore(jdbcTokenStore(nodeId));
        SlowParticipant participant = new SlowParticipant();
        TransactionalUnitOfWorkFactory unitOfWorkFactory =
                new TransactionalUnitOfWorkFactory(jdbcTransactionManager(dataSource),
                                                   UnitOfWorkTestUtils.SIMPLE_FACTORY);
        var module = EventProcessorModule.pooledStreaming(PROCESSOR_NAME)
                                         .eventHandlingComponents(c -> c.autodetected("p", cfg -> participant))
                                         .customized((cfg, c) -> {
                                             var customized = c.eventSource(eventSource)
                                                               .tokenStore(store)
                                                               .unitOfWorkFactory(unitOfWorkFactory)
                                                               .initialSegmentCount(1)
                                                               .claimExtensionThreshold(300)
                                                               .tokenClaimInterval(200);
                                             return coordinatorExtendsClaims
                                                     ? customized.enableCoordinatorClaimExtension()
                                                     : customized;
                                         });
        AxonConfiguration configuration = MessagingConfigurer.create()
                                                             .eventProcessing(ep -> ep.pooledStreaming(
                                                                     ps -> ps.processor(module)))
                                                             .build();
        nodes.add(configuration);
        configuration.start();
        return new Node(nodeId, store, participant);
    }

    private JdbcTokenStore jdbcTokenStore(String nodeId) {
        return new JdbcTokenStore(new JdbcTransactionalExecutorProvider(dataSource),
                                  new JacksonConverter(),
                                  JdbcTokenStoreConfiguration.DEFAULT.claimTimeout(CLAIM_TIMEOUT).nodeId(nodeId));
    }

    private long sinceBegin(long nanos) {
        return nanos == 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(nanos - begin);
    }

    private record Node(String nodeId, RecordingTokenStore store, SlowParticipant participant) {

    }

    private final class Contest {

        private final String summary;

        private Contest(Node first, Node second) {
            this.summary = "waitStartMs=" + sinceBegin(first.participant.slowStartedAt.get())
                    + " waitEndMs=" + sinceBegin(first.participant.slowCompletedAt.get())
                    + " secondClaimedMs=" + sinceBegin(second.store.claimedAt.get())
                    + " secondHandled=" + second.participant.handled
                    + " firstStoreFailures=" + first.store.storeFailures
                    + " firstExtendClaims=" + first.store.extendClaims
                    + " firstMaxExtendClaimMs=" + first.store.maxExtendClaimMillis.get();
        }

        String summary() {
            return summary;
        }
    }

    static final class SlowParticipant implements Checkpointing {

        final List<String> handled = new CopyOnWriteArrayList<>();
        final AtomicBoolean slowOnce = new AtomicBoolean();
        final AtomicLong slowStartedAt = new AtomicLong();
        final AtomicLong slowCompletedAt = new AtomicLong();

        @EventHandler
        void on(String event, CheckpointTrigger checkpoint) {
            handled.add(event);
            checkpoint.requestCheckpoint();
        }

        @Override
        public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
            if (!slowOnce.compareAndSet(true, false)) {
                return CompletableFuture.completedFuture(requested);
            }
            slowStartedAt.set(System.nanoTime());
            return CompletableFuture.supplyAsync(() -> {
                slowCompletedAt.set(System.nanoTime());
                return requested;
            }, CompletableFuture.delayedExecutor(SLOW_CHECKPOINT.toMillis(), TimeUnit.MILLISECONDS));
        }
    }

    final class RecordingTokenStore implements TokenStore {

        private final JdbcTokenStore delegate;
        final AtomicInteger stores = new AtomicInteger();
        final List<String> storeFailures = new CopyOnWriteArrayList<>();
        final List<String> extendClaims = new CopyOnWriteArrayList<>();
        final AtomicLong maxExtendClaimMillis = new AtomicLong();
        final AtomicLong claimedAt = new AtomicLong();

        RecordingTokenStore(JdbcTokenStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<List<Segment>> initializeTokenSegments(String processorName, int segmentCount,
                                                                        @Nullable TrackingToken initialToken,
                                                                        @Nullable ProcessingContext context) {
            return delegate.initializeTokenSegments(processorName, segmentCount, initialToken, context);
        }

        @Override
        public CompletableFuture<Void> storeToken(@Nullable TrackingToken token, String processorName, int segmentId,
                                                  @Nullable ProcessingContext context) {
            return delegate.storeToken(token, processorName, segmentId, context).whenComplete((ok, error) -> {
                if (error == null) {
                    stores.incrementAndGet();
                } else {
                    Throwable cause = error.getCause() != null ? error.getCause() : error;
                    storeFailures.add(sinceBegin(System.nanoTime()) + "ms " + cause.getClass().getSimpleName());
                }
            });
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName, int segmentId,
                                                           @Nullable ProcessingContext context) {
            return recordClaim(delegate.fetchToken(processorName, segmentId, context));
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName, Segment segment,
                                                           @Nullable ProcessingContext context) {
            return recordClaim(delegate.fetchToken(processorName, segment, context));
        }

        private CompletableFuture<TrackingToken> recordClaim(CompletableFuture<TrackingToken> claim) {
            return claim.whenComplete((token, error) -> {
                if (error == null) {
                    claimedAt.compareAndSet(0, System.nanoTime());
                }
            });
        }

        @Override
        public CompletableFuture<Void> extendClaim(String processorName, int segmentId,
                                                   @Nullable ProcessingContext context) {
            long start = System.nanoTime();
            return delegate.extendClaim(processorName, segmentId, context).whenComplete((ok, error) -> {
                long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                maxExtendClaimMillis.accumulateAndGet(millis, Math::max);
                extendClaims.add(sinceBegin(start) + "ms+" + millis + (error == null ? "" : "!"
                        + error.getClass().getSimpleName()) + "@" + Thread.currentThread().getName());
            });
        }

        @Override
        public CompletableFuture<Void> releaseClaim(String processorName, int segmentId,
                                                    @Nullable ProcessingContext context) {
            return delegate.releaseClaim(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Void> initializeSegment(@Nullable TrackingToken token, String processorName,
                                                         Segment segment, @Nullable ProcessingContext context) {
            return delegate.initializeSegment(token, processorName, segment, context);
        }

        @Override
        public CompletableFuture<Void> deleteToken(String processorName, int segmentId,
                                                   @Nullable ProcessingContext context) {
            return delegate.deleteToken(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Segment> fetchSegment(String processorName, int segmentId,
                                                       @Nullable ProcessingContext context) {
            return delegate.fetchSegment(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<List<Segment>> fetchSegments(String processorName,
                                                              @Nullable ProcessingContext context) {
            return delegate.fetchSegments(processorName, context);
        }

        @Override
        public CompletableFuture<List<Segment>> fetchAvailableSegments(String processorName,
                                                                       @Nullable ProcessingContext context) {
            return delegate.fetchAvailableSegments(processorName, context);
        }

        @Override
        public CompletableFuture<String> retrieveStorageIdentifier(@Nullable ProcessingContext context) {
            return delegate.retrieveStorageIdentifier(context);
        }
    }

    private static TransactionManager jdbcTransactionManager(DataSource dataSource) {
        return new TransactionManager() {
            @Override
            public Transaction startTransaction() {
                return new Transaction() {
                    @Override
                    public void commit() {
                    }

                    @Override
                    public void rollback() {
                    }
                };
            }

            @Override
            public void attachToProcessingLifecycle(ProcessingLifecycle processingLifecycle) {
                processingLifecycle.runOnPreInvocation(pc -> {
                    LazyConnection connection = new LazyConnection(dataSource);
                    Supplier<ConnectionExecutor> executor =
                            CachingSupplier.of(() -> new ConnectionExecutor(connection));
                    pc.putResource(JdbcTransactionalExecutorProvider.SUPPLIER_KEY, executor);
                    pc.onCommit(p -> {
                        try {
                            connection.commit();
                            return CompletableFuture.completedFuture(null);
                        } catch (SQLException e) {
                            return CompletableFuture.failedFuture(e);
                        } finally {
                            connection.close();
                        }
                    });
                    pc.onError((p, phase, e) -> {
                        try {
                            connection.rollback();
                        } catch (SQLException se) {
                            e.addSuppressed(se);
                        } finally {
                            connection.close();
                        }
                    });
                });
            }
        };
    }

    private static final class LazyConnection implements ConnectionProvider {

        private final DataSource dataSource;
        private @Nullable Connection connection;

        private LazyConnection(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public synchronized Connection getConnection() throws SQLException {
            if (connection == null) {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
            }
            return connection;
        }

        synchronized void commit() throws SQLException {
            if (connection != null) {
                connection.commit();
            }
        }

        synchronized void rollback() throws SQLException {
            if (connection != null) {
                connection.rollback();
            }
        }

        synchronized void close() {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                connection = null;
            }
        }
    }
}
