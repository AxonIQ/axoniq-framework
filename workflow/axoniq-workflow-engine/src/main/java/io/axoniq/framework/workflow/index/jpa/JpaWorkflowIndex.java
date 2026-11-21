/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.WorkflowIndexEntry.SegmentKeyRange;
import io.axoniq.framework.workflow.runtime.util.WorkflowIds;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;

/**
 * A {@link WorkflowIndex} keeping the index of running workflow instances in a relational database through JPA.
 * <p>
 * With a durable index shared by every node, a restarted application, or another node taking over a segment, restores
 * the running workflow instances of each segment from the stored index plus the lifecycle events after the token the
 * segment resumes from, instead of from every workflow lifecycle event ever written. The index records no position of
 * its own: every checkpoint of the workflow engine's event processor writes the pending index changes before it stores
 * the token, see {@link WorkflowIndex}. While the database is unavailable, the checkpoints of the segments with pending
 * changes therefore fail, and the processor claims those segments again after a back-off. The event store remains the
 * source of truth: emptying the tables, the entries together with the initialization, only makes the next claims
 * slower, as the first of them reads the running instances from the event store and initializes the index again.
 * <p>
 * The index uses two entities, which the persistence unit must include:
 * <ul>
 *     <li>{@link WorkflowIndexEntry}: one row per running workflow instance, with primary key
 *     ({@code indexName VARCHAR(255)}, {@code workflowId VARCHAR(255)}), a {@code segmentKey BIGINT NOT NULL} column
 *     and an index on ({@code indexName}, {@code segmentKey}).</li>
 *     <li>{@link WorkflowIndexInitialization}: one row per initialized index, with primary key
 *     {@code indexName VARCHAR(255)}.</li>
 * </ul>
 * Table and column names follow the JPA provider's naming strategy. The index does not create or migrate them. In
 * Spring Boot, the entities are registered with the persistence unit once the index is selected. The
 * {@code workflowId} column holds the valid workflow ids the engine starts instances with, see {@link WorkflowIds}.
 * <p>
 * The index deletes and inserts its entries by workflow id, so the {@code workflowId} column must compare ids exactly:
 * case-sensitively, and without ignoring trailing spaces, as a binary collation does. Under a case-insensitive or
 * pad-space collation, such as MySQL's default {@code utf8mb4_0900_ai_ci} or SQL Server's default case-insensitive
 * collations, the entries of {@code ORDER-1} and {@code order-1}, or of {@code order-1} and {@code "order-1 "}, would
 * overwrite each other, and a running instance would silently drop out of the index. When such ids are written
 * together, they fail on a duplicate key instead: at initialization, which keeps the index uninitialized, or in one
 * checkpoint of the segment owning both, which fails that checkpoint. On MySQL 8.0.17 or later, declare the column as
 * {@code VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin}, which is case-sensitive and does not pad;
 * {@code utf8mb4_bin} and {@code ascii_bin} are case-sensitive, but ignore trailing spaces. On SQL Server, use a
 * {@code _BIN2} collation, such as {@code VARCHAR(255) COLLATE Latin1_General_100_BIN2}, to compare case-sensitively;
 * SQL Server ignores trailing spaces when comparing strings under every collation, so there, do not use workflow ids
 * that differ only in trailing spaces. The default collations of PostgreSQL and H2 already compare exactly.
 * <p>
 * A running instance with an invalid id, for example one started by an earlier version of the engine, cannot be
 * written: the index rejects the write with an error naming the id. The segment owning that instance then makes no
 * progress: every checkpoint fails, so its token does not advance past the first event it handles, and the processor
 * claims it again and reprocesses the same events, until that instance is cancelled or has ended, or the index is
 * disabled. As initialization receives the id of every running instance, the index also stays uninitialized
 * meanwhile, so every claim of every segment reads the full running-workflow list.
 * <p>
 * <h2>Transactions</h2>
 * The writes of a checkpoint join the transaction in which the event processor stores its token, whenever the
 * {@link TransactionalExecutorProvider} finds one bound to the processing context of that checkpoint: with a
 * {@link JpaTransactionalExecutorProvider}, whenever the processor's transaction manager binds an
 * {@link EntityManager} to its processing contexts, as the Spring transaction manager and the
 * {@link org.axonframework.messaging.core.unitofwork.transaction.jpa.EntityManagerTransactionManager} do. The index then
 * writes on the processor's thread, through the processor's {@link EntityManager}, so the changes and the token are
 * committed together, and a checkpoint uses no database connection besides the one the processor already holds. This
 * is the same rule the JPA token store follows, and it assumes that the index lives in the database of that
 * transaction. To keep the index in another database, configure a {@link TransactionalExecutorProvider} that ignores
 * the processing context, for example {@code context -> provider.getTransactionalExecutor(null)}.
 * <p>
 * Every other operation, and a checkpoint without such a transaction, runs in a transaction of its own, obtained from
 * the {@link TransactionalExecutorProvider} without a processing context, on the configured {@link Executor}: the
 * workflow engine calls the index from its event handling and workflow threads, and expects a future rather than
 * blocking JDBC work on those threads. Such a write needs a database connection of its own. When the event processor
 * holds a connection of the same pool while it awaits the checkpoint, for example in a JDBC transaction the index
 * cannot join, size the pool for at least one connection per segment the node processes at once, plus the
 * {@value #DEFAULT_THREAD_COUNT} threads of the index; otherwise checkpoints may wait for connections their own
 * processors hold, and fail once their writes time out. A returned future completes only once its transaction has
 * ended, committed or rolled back, so a write never takes effect after its future reported it as failed, and the
 * engine issues the next write of a segment only once the previous one can no longer hold a connection. An operation
 * that has not started its transaction within the configured timeout fails and is skipped when the executor gets to
 * it. Once started, the queries of the transaction are bounded by the same timeout through the
 * {@code jakarta.persistence.query.timeout} hint, also when it joins the processor's transaction; acquiring a
 * connection, and the statements the JPA provider issues when it flushes the transaction, are bounded by the connection
 * pool and JDBC driver configuration instead. An initialization adds its entries and marks the index in the same
 * transaction, so a load never sees the mark without the entries. Two claims initializing an index at the same time
 * may conflict on the mark; the losing claim then writes its entries with its later writes.
 * <p>
 * Construct the index through its {@link #builder() builder} and register it with the
 * {@link io.axoniq.framework.workflow.configuration.WorkflowConfigurer}:
 * <pre>{@code
 * WorkflowConfigurer.create()
 *                   .registerWorkflowIndex(config -> JpaWorkflowIndex.builder()
 *                                                    .entityManagerFactory(entityManagerFactory)
 *                                                    .build());
 * }</pre>
 * In Spring Boot, the auto-configuration registers a {@code JpaWorkflowIndex} when the application sets
 * {@code axon.workflow.index=jpa}, an {@code EntityManagerFactory} bean is present, and no other {@code WorkflowIndex}
 * bean is defined. Select it only once every node that runs the workflow modules uses it.
 * <p>
 * Marked {@link Internal} as a warning that API stability is not promised yet: this class is usable, and supported
 * for configuring the workflow index and for custom implementations, but its shape may still change in a minor
 * release.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
public class JpaWorkflowIndex implements WorkflowIndex {

    /**
     * The default time an operation may wait to start, and the default bound on each of its queries.
     */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

    /**
     * The number of threads of the executor the index creates when none is configured.
     */
    public static final int DEFAULT_THREAD_COUNT = 4;

    /**
     * The number of operations that may wait for a thread of the executor the index creates when none is configured.
     */
    public static final int DEFAULT_QUEUE_CAPACITY = 1024;
    private static final int MAX_IDS_PER_STATEMENT = 500;
    private static final String QUERY_TIMEOUT_HINT = "jakarta.persistence.query.timeout";

    private final TransactionalExecutorProvider<EntityManager> transactionalExecutorProvider;
    private final Executor executor;
    private final Duration timeout;
    // An Integer, which is what JPA providers accept for the query timeout hint.
    private final int queryTimeoutMillis;
    // Creates the deadline by which a piece of work must have started, which fails once the timeout passed.
    // Package-private so a test can make that deadline pass at a moment of its choosing, instead of waiting for it.
    Supplier<CompletableFuture<Void>> startDeadlines;

    /**
     * Instantiates a {@code JpaWorkflowIndex} based on the fields contained in the given {@link Builder}.
     * <p>
     * Will assert that the {@link TransactionalExecutorProvider} is set, and throw an
     * {@link AxonConfigurationException} if it is missing.
     *
     * @param builder the {@link Builder} used to instantiate a {@code JpaWorkflowIndex}
     * @throws AxonConfigurationException if the transactional executor provider is missing
     */
    protected JpaWorkflowIndex(Builder builder) {
        builder.validate();
        this.transactionalExecutorProvider = requireNonNull(builder.transactionalExecutorProvider);
        this.executor = builder.executor != null ? builder.executor : defaultExecutor();
        this.timeout = builder.timeout;
        this.queryTimeoutMillis = (int) Math.min(Integer.MAX_VALUE, timeout.toMillis());
        this.startDeadlines = () -> new CompletableFuture<Void>().orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Instantiates a {@link Builder} to construct a {@code JpaWorkflowIndex}.
     * <p>
     * Either an {@link Builder#entityManagerFactory(EntityManagerFactory) EntityManagerFactory} or a
     * {@link Builder#transactionalExecutorProvider(TransactionalExecutorProvider) TransactionalExecutorProvider} is
     * mandatory. The {@link Builder#executor(Executor) executor}
     * defaults to a pool of {@value #DEFAULT_THREAD_COUNT} daemon threads, and the
     * {@link Builder#timeout(Duration) timeout} to {@link #DEFAULT_TIMEOUT}.
     *
     * @return a {@link Builder} to construct a {@code JpaWorkflowIndex}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates the executor used when none is configured: a pool of {@value #DEFAULT_THREAD_COUNT} daemon threads, which
     * bounds the database connections the index uses at once. At most {@value #DEFAULT_QUEUE_CAPACITY} operations wait
     * for a thread; further operations fail right away. The engine issues one write per segment at a time and one load
     * per claim, so only operations that timed out while waiting, and are skipped once their turn comes, can fill the
     * queue, when the database stalls.
     */
    private static Executor defaultExecutor() {
        var pool = new ThreadPoolExecutor(DEFAULT_THREAD_COUNT,
                                          DEFAULT_THREAD_COUNT,
                                          60,
                                          TimeUnit.SECONDS,
                                          new LinkedBlockingQueue<>(DEFAULT_QUEUE_CAPACITY),
                                          Thread.ofPlatform().daemon().name("workflow-index-jpa-", 0).factory());
        // Idle threads end, so an index that is no longer used holds no threads and needs no shutdown.
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    @Override
    public CompletableFuture<Optional<Set<String>>> load(String indexName, Segment segment) {
        requireNonNull(indexName, "The index name must not be null.");
        requireNonNull(segment, "The segment must not be null.");
        return inTransaction(transaction -> transaction.apply(em -> {
            if (em.find(WorkflowIndexInitialization.class, indexName, hints()) == null) {
                return Optional.empty();
            }
            return Optional.of(ownedWorkflowIds(em, indexName, segment));
        }));
    }

    private Set<String> ownedWorkflowIds(EntityManager em, String indexName, Segment segment) {
        SegmentKeyRange range = WorkflowIndexEntry.rangeOf(segment);
        return Set.copyOf(
                em.createQuery("select e.workflowId from WorkflowIndexEntry e "
                                       + "where e.indexName = :indexName "
                                       + "and e.segmentKey between :lowest and :highest",
                               String.class)
                  .setHint(QUERY_TIMEOUT_HINT, queryTimeoutMillis)
                  .setParameter("indexName", indexName)
                  .setParameter("lowest", range.lowest())
                  .setParameter("highest", range.highest())
                  .getResultList()
        );
    }

    @Override
    public CompletableFuture<Void> store(String indexName, IndexDelta delta) {
        requireNonNull(indexName, "The index name must not be null.");
        requireNonNull(delta, "The delta must not be null.");
        return inTransaction(transaction -> transaction.accept(em -> apply(em, indexName, delta)));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Writes within the transaction of the given {@code context}, on the calling thread, when the
     * {@link TransactionalExecutorProvider} finds a transactional executor bound to it. Otherwise, writes in a
     * transaction of its own, as {@link #store(String, IndexDelta)} does.
     */
    @Override
    public CompletableFuture<Void> store(String indexName, IndexDelta delta, ProcessingContext context) {
        requireNonNull(indexName, "The index name must not be null.");
        requireNonNull(delta, "The delta must not be null.");
        requireNonNull(context, "The processing context must not be null.");
        TransactionalExecutor<EntityManager> joined;
        try {
            joined = transactionalExecutorProvider.findTransactionalExecutor(context);
            if (joined == null) {
                return store(indexName, delta);
            }
            return joined.accept(em -> apply(em, indexName, delta));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> initialize(String indexName, Set<String> runningWorkflowIds) {
        requireNonNull(indexName, "The index name must not be null.");
        IndexDelta delta = new IndexDelta(runningWorkflowIds, Set.of());
        return inTransaction(transaction -> transaction.accept(em -> {
            apply(em, indexName, delta);
            if (em.find(WorkflowIndexInitialization.class, indexName, hints()) == null) {
                persistDetached(em, List.of(new WorkflowIndexInitialization(indexName)));
            }
        }));
    }

    private void apply(EntityManager em, String indexName, IndexDelta delta) {
        requireValidWorkflowIds(delta.added());
        // Deleting the added ids too makes the write idempotent: an id may already be stored by an earlier write that
        // was reported as failed but committed anyway.
        Set<String> touched = new HashSet<>(delta.removed());
        touched.addAll(delta.added());
        for (List<String> ids : partition(touched)) {
            em.createQuery("delete from WorkflowIndexEntry e "
                                   + "where e.indexName = :indexName and e.workflowId in :workflowIds")
              .setHint(QUERY_TIMEOUT_HINT, queryTimeoutMillis)
              .setParameter("indexName", indexName)
              .setParameter("workflowIds", ids)
              .executeUpdate();
        }
        persistDetached(em, delta.added().stream().map(id -> new WorkflowIndexEntry(indexName, id)).toList());
    }

    /**
     * Rejects a write that adds an invalid workflow id, see {@link WorkflowIds}, naming the id: the database may
     * reject it without naming it, or, depending on its settings, store a truncated id. The engine does not start an
     * instance with an invalid id, so only an instance started before the rules applied can have one.
     */
    private static void requireValidWorkflowIds(Set<String> workflowIds) {
        for (String workflowId : workflowIds) {
            WorkflowIds.invalidity(workflowId).ifPresent(reason -> {
                throw new IllegalArgumentException(
                        "Cannot write workflow id '" + WorkflowIds.abbreviate(workflowId) + "' to the workflow index: "
                                + "it is not a valid workflow id, as " + reason + "."
                );
            });
        }
    }

    /**
     * Inserts the given entities and detaches them again, so they never stay in the persistence context: a later
     * transaction of the same {@link EntityManager}, as an {@code EntityManagerProvider} shared by every transaction
     * provides, could otherwise not insert an entity with the same id once the bulk delete removed its row. The
     * persistence context itself is not cleared, as it may hold the entities of others, such as the token store's.
     */
    private static void persistDetached(EntityManager em, List<?> entities) {
        if (entities.isEmpty()) {
            return;
        }
        try {
            entities.forEach(em::persist);
            em.flush();
        } finally {
            entities.forEach(em::detach);
        }
    }

    private static List<List<String>> partition(Set<String> ids) {
        List<String> all = List.copyOf(ids);
        List<List<String>> parts = new ArrayList<>();
        for (int from = 0; from < all.size(); from += MAX_IDS_PER_STATEMENT) {
            parts.add(all.subList(from, Math.min(all.size(), from + MAX_IDS_PER_STATEMENT)));
        }
        return parts;
    }

    private Map<String, Object> hints() {
        return Map.of(QUERY_TIMEOUT_HINT, queryTimeoutMillis);
    }

    /**
     * Runs the given work, which runs a transaction of its own on the given transactional executor, on the index's
     * executor.
     * <p>
     * The returned future fails when the work has not started within the timeout, and the work is then skipped. Once
     * started, the future completes when the work's transaction has ended, so it never reports a transaction as
     * settled that may still commit.
     */
    private <R> CompletableFuture<R> inTransaction(
            Function<TransactionalExecutor<EntityManager>, CompletableFuture<R>> work
    ) {
        CompletableFuture<R> result = new CompletableFuture<>();
        // Claimed by whichever comes first: the work starting, or the timeout of the work waiting to start.
        AtomicBoolean claimed = new AtomicBoolean();
        // Completing it once the work starts cancels its timer, so the timer does not keep the work alive.
        CompletableFuture<Void> startDeadline = startDeadlines.get();
        startDeadline.whenComplete((started, timedOut) -> {
            if (timedOut != null && claimed.compareAndSet(false, true)) {
                result.completeExceptionally(new TimeoutException(
                        "The work did not start within " + timeout + "; it is skipped."
                ));
            }
        });
        try {
            executor.execute(() -> {
                if (!claimed.compareAndSet(false, true)) {
                    return;
                }
                startDeadline.complete(null);
                try {
                    work.apply(transactionalExecutorProvider.getTransactionalExecutor(null))
                        .whenComplete((value, failure) -> {
                            if (failure != null) {
                                result.completeExceptionally(failure);
                            } else {
                                result.complete(value);
                            }
                        });
                } catch (RuntimeException e) {
                    result.completeExceptionally(e);
                }
            });
        } catch (RuntimeException e) {
            // The executor rejected the work.
            startDeadline.complete(null);
            return CompletableFuture.failedFuture(e);
        }
        return result;
    }

    /**
     * Builder class to instantiate a {@link JpaWorkflowIndex}.
     * <p>
     * Either an {@link #entityManagerFactory(EntityManagerFactory) EntityManagerFactory} or a
     * {@link #transactionalExecutorProvider(TransactionalExecutorProvider) TransactionalExecutorProvider} is
     * mandatory. The {@link #executor(Executor) executor} defaults to a
     * pool of {@value JpaWorkflowIndex#DEFAULT_THREAD_COUNT} daemon threads, and the
     * {@link #timeout(Duration) timeout} defaults to {@link JpaWorkflowIndex#DEFAULT_TIMEOUT}.
     */
    public static class Builder {

        private @Nullable TransactionalExecutorProvider<EntityManager> transactionalExecutorProvider;
        private @Nullable Executor executor;
        private Duration timeout = DEFAULT_TIMEOUT;

        /**
         * Sets the {@link EntityManagerFactory} the index obtains the {@link EntityManager} of each transaction of its
         * own from, through a {@link JpaTransactionalExecutorProvider}. The writes of a checkpoint join the
         * transaction of the event processor instead, when its transaction manager binds an {@link EntityManager} to
         * the processing context. Replaces a previously configured
         * {@link #transactionalExecutorProvider(TransactionalExecutorProvider) TransactionalExecutorProvider}.
         *
         * @param entityManagerFactory the {@link EntityManagerFactory} of the persistence unit holding the index
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder entityManagerFactory(EntityManagerFactory entityManagerFactory) {
            assertNonNull(entityManagerFactory, "The EntityManagerFactory may not be null.");
            return transactionalExecutorProvider(new JpaTransactionalExecutorProvider(entityManagerFactory));
        }

        /**
         * Sets the {@link TransactionalExecutorProvider} providing the transactions the index runs its work in. The
         * writes of a checkpoint use the transactional executor it finds bound to the processing context of the
         * checkpoint, joining the transaction the event processor stores its token in; every other operation asks for
         * one without a processing context, which runs in a transaction of its own. Replaces a previously configured
         * {@link #entityManagerFactory(EntityManagerFactory) EntityManagerFactory}.
         *
         * @param transactionalExecutorProvider the provider of the transactions the index runs its work in
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder transactionalExecutorProvider(
                TransactionalExecutorProvider<EntityManager> transactionalExecutorProvider
        ) {
            assertNonNull(transactionalExecutorProvider, "The TransactionalExecutorProvider may not be null.");
            this.transactionalExecutorProvider = transactionalExecutorProvider;
            return this;
        }

        /**
         * Sets the {@link Executor} running the blocking database work of the index. Its number of threads bounds the
         * database connections the index uses at once. Defaults to a pool of
         * {@value JpaWorkflowIndex#DEFAULT_THREAD_COUNT} daemon threads that end when idle, with a queue of at most
         * {@value JpaWorkflowIndex#DEFAULT_QUEUE_CAPACITY} operations.
         *
         * @param executor the executor running the blocking database work
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder executor(Executor executor) {
            assertNonNull(executor, "The Executor may not be null.");
            this.executor = executor;
            return this;
        }

        /**
         * Sets the time an operation may wait for the executor before it fails and is skipped, which also bounds each
         * query of a started operation. A started operation completes its future once its transaction has ended,
         * whatever the timeout. Defaults to {@link JpaWorkflowIndex#DEFAULT_TIMEOUT}.
         *
         * @param timeout the bound on starting an operation and on each of its queries, positive
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder timeout(Duration timeout) {
            assertNonNull(timeout, "The timeout may not be null.");
            assertThat(timeout, t -> !t.isNegative() && !t.isZero(),
                       "The timeout must be positive, but was " + timeout + ".");
            this.timeout = timeout;
            return this;
        }

        /**
         * Initializes a {@link JpaWorkflowIndex} as specified through this Builder.
         *
         * @return a {@link JpaWorkflowIndex} as specified through this Builder
         */
        public JpaWorkflowIndex build() {
            return new JpaWorkflowIndex(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if the transactional executor provider is missing
         */
        protected void validate() {
            assertNonNull(transactionalExecutorProvider,
                          "Either an EntityManagerFactory or a TransactionalExecutorProvider is mandatory.");
        }
    }
}
