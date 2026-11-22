/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.IndexDelta;
import io.axoniq.framework.workflow.index.api.WorkflowIndexContractTest;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.function.ThrowingFunction;
import org.axonframework.common.jpa.SimpleEntityManagerProvider;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.EntityManagerTransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link JpaWorkflowIndex}, against an in-memory HSQLDB database.
 *
 * @author Allard Buijze
 */
class JpaWorkflowIndexTest extends WorkflowIndexContractTest {

    private static final String INDEX = "orders";

    private final EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory(
            "workflowIndex", Map.of("jakarta.persistence.jdbc.url", "jdbc:hsqldb:mem:" + UUID.randomUUID())
    );

    @AfterEach
    void closeDatabase() {
        entityManagerFactory.close();
    }

    @Override
    protected WorkflowIndex createIndex() {
        return JpaWorkflowIndex.builder()
                               .entityManagerFactory(entityManagerFactory)
                               .build();
    }

    private static <T> T await(CompletableFuture<T> future) {
        return future.orTimeout(5, TimeUnit.SECONDS).join();
    }

    @Nested
    class ValidWorkflowIds {

        @Test
        void holdsAWorkflowIdOfTheMaximumLength() {
            // given
            var workflowIndex = createIndex();
            var longest = "w".repeat(255);

            // when
            await(workflowIndex.initialize(INDEX, Set.of(longest)));

            // then
            assertThat(await(workflowIndex.load(INDEX, Segment.ROOT_SEGMENT))).hasValue(Set.of(longest));
        }

        @Test
        void rejectsAWriteAddingAnInvalidWorkflowIdWithAnErrorNamingTheId() {
            // given - a running workflow started with an id longer than the rules allow
            var workflowIndex = createIndex();
            await(workflowIndex.initialize(INDEX, Set.of("order-1")));
            var tooLong = "order-" + "w".repeat(250);

            // when
            var write = workflowIndex.store(INDEX, new IndexDelta(Set.of("order-2", tooLong), Set.of("order-1")));

            // then - the error names the start of the id, and the write changed nothing
            assertThatThrownBy(() -> await(write))
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .hasMessageContaining("order-" + "w".repeat(58) + "...")
                    .hasMessageContaining("256 characters");
            assertThat(await(workflowIndex.load(INDEX, Segment.ROOT_SEGMENT))).hasValue(Set.of("order-1"));
        }

        @Test
        void rejectsAnInitializationWithANonAsciiWorkflowId() {
            // given
            var workflowIndex = createIndex();

            // when
            var initialization = workflowIndex.initialize(INDEX, Set.of("order-1", "bestellung-\u00fc"));

            // then
            assertThatThrownBy(() -> await(initialization))
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .hasMessageContaining("bestellung-\\u00FC");
            assertThat(await(workflowIndex.load(INDEX, Segment.ROOT_SEGMENT))).isEmpty();
        }
    }

    @Nested
    class Transactions {

        private static final String REJECTED = "rejected";

        @BeforeEach
        void rejectAnEntry() {
            // A database constraint rejecting one workflow id, so a write fails after its removals ran.
            try (EntityManager em = entityManagerFactory.createEntityManager()) {
                em.getTransaction().begin();
                em.createNativeQuery("alter table WorkflowIndexEntry add constraint rejectsAnEntry "
                                             + "check (workflowId <> '" + REJECTED + "')")
                  .executeUpdate();
                em.getTransaction().commit();
            }
        }

        @Test
        void aFailingWriteChangesNoEntry() {
            // given
            var workflowIndex = createIndex();
            await(workflowIndex.initialize(INDEX, Set.of("order-1")));

            // when - inserting an id the database rejects fails after the removal ran within the transaction
            var write = workflowIndex.store(INDEX, new IndexDelta(Set.of("order-2", REJECTED), Set.of("order-1")));

            // then
            assertThatThrownBy(() -> await(write)).isInstanceOf(CompletionException.class);
            assertThat(await(workflowIndex.load(INDEX, Segment.ROOT_SEGMENT))).hasValue(Set.of("order-1"));
        }

        @Test
        void aFailingInitializationNeitherAddsEntriesNorMarksTheIndex() {
            // given
            var workflowIndex = createIndex();

            // when
            var initialization = workflowIndex.initialize(INDEX, Set.of("order-1", REJECTED));

            // then
            assertThatThrownBy(() -> await(initialization)).isInstanceOf(CompletionException.class);
            assertThat(await(workflowIndex.load(INDEX, Segment.ROOT_SEGMENT))).isEmpty();
            // and - initializing it afterward shows that the entry of order-1 was not added either
            await(workflowIndex.initialize(INDEX, Set.of()));
            assertThat(await(workflowIndex.load(INDEX, Segment.ROOT_SEGMENT))).hasValue(Set.of());
        }
    }

    @Nested
    class WithinAProcessingContext {

        private final EntityManager entityManager = entityManagerFactory.createEntityManager();
        // Binds a JPA transaction to every processing context, as the transaction manager of an event processor does.
        private final UnitOfWorkFactory transactional = new TransactionalUnitOfWorkFactory(
                new EntityManagerTransactionManager(new SimpleEntityManagerProvider(entityManager)),
                new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE)
        );

        @AfterEach
        void closeEntityManager() {
            entityManager.close();
        }

        @Test
        void writesWithinTheTransactionOfTheContextOnTheCallingThread() {
            // given
            var workflowIndex = createIndex();
            await(workflowIndex.initialize(INDEX, Set.of("order-1")));

            // when
            await(transactional.create().executeWithResult(context -> {
                var write = workflowIndex.store(INDEX, new IndexDelta(Set.of("order-2"), Set.of("order-1")), context);
                // then - the write ran within the context's transaction before the store returned
                assertThat(write).isCompleted();
                return write;
            }));

            // then - it is durable once the context committed
            assertThat(await(workflowIndex.load(INDEX, Segment.ROOT_SEGMENT))).hasValue(Set.of("order-2"));
        }

        @Test
        void writesNothingWhenTheContextRollsBack() {
            // given
            var workflowIndex = createIndex();
            await(workflowIndex.initialize(INDEX, Set.of("order-1")));

            // when - the context fails after the write
            var execution = transactional.create().executeWithResult(context -> workflowIndex
                    .store(INDEX, new IndexDelta(Set.of("order-2"), Set.of("order-1")), context)
                    .thenCompose(written -> CompletableFuture.failedFuture(new IllegalStateException("rolled back"))));

            // then
            assertThatThrownBy(() -> await(execution)).isInstanceOf(CompletionException.class);
            assertThat(await(workflowIndex.load(INDEX, Segment.ROOT_SEGMENT))).hasValue(Set.of("order-1"));
        }

        @Test
        void writesTheSameWorkflowAgainInLaterTransactionsOfTheSameEntityManager() {
            // given - an entity manager shared by every transaction, as a single EntityManagerProvider does
            var workflowIndex = createIndex();
            await(workflowIndex.initialize(INDEX, Set.of()));
            List<IndexDelta> deltas = List.of(new IndexDelta(Set.of("order-1"), Set.of()),
                                              new IndexDelta(Set.of("order-1"), Set.of()),
                                              new IndexDelta(Set.of(), Set.of("order-1")),
                                              new IndexDelta(Set.of("order-1"), Set.of()));

            // when - each write runs in a later transaction of that entity manager
            deltas.forEach(delta -> await(transactional.create().executeWithResult(
                    context -> workflowIndex.store(INDEX, delta, context)
            )));

            // then
            assertThat(await(workflowIndex.load(INDEX, Segment.ROOT_SEGMENT))).hasValue(Set.of("order-1"));
        }

        @Test
        void writesInATransactionOfItsOwnWhenTheContextHasNoneToJoin() {
            // given
            var workflowIndex = createIndex();
            await(workflowIndex.initialize(INDEX, Set.of("order-1")));
            var withoutTransaction = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE);

            // when - the context fails after the write, which did not join it
            var execution = withoutTransaction.create().executeWithResult(context -> workflowIndex
                    .store(INDEX, new IndexDelta(Set.of("order-2"), Set.of()), context)
                    .thenCompose(written -> CompletableFuture.failedFuture(new IllegalStateException("rolled back"))));

            // then
            assertThatThrownBy(() -> await(execution)).isInstanceOf(CompletionException.class);
            assertThat(await(workflowIndex.load(INDEX, Segment.ROOT_SEGMENT))).hasValue(Set.of("order-1", "order-2"));
        }
    }

    @Nested
    class Durability {

        @Test
        void survivesANewIndexOnTheSameDatabase() {
            // given
            await(createIndex().initialize(INDEX, Set.of("order-1")));

            // when
            var index = await(createIndex().load(INDEX, Segment.ROOT_SEGMENT));

            // then
            assertThat(index).hasValue(Set.of("order-1"));
        }
    }

    @Nested
    class Execution {

        @Test
        void boundsItsFuturesWhenTheWorkDoesNotRun() {
            // given - an executor that never runs the work
            Executor neverRuns = task -> {
            };
            var workflowIndex = JpaWorkflowIndex.builder()
                                                .entityManagerFactory(entityManagerFactory)
                                                .executor(neverRuns)
                                                .timeout(Duration.ofMillis(100))
                                                .build();

            // when
            var load = workflowIndex.load(INDEX, Segment.ROOT_SEGMENT);
            var write = workflowIndex.store(INDEX, new IndexDelta(Set.of(), Set.of()));

            // then
            assertThatThrownBy(() -> load.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
            assertThatThrownBy(() -> write.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
        }

        @Test
        void skipsWorkThatDidNotStartWithinTheTimeout() {
            // given - an executor that holds back the work until the test runs it
            List<Runnable> heldBack = new CopyOnWriteArrayList<>();
            var workflowIndex = JpaWorkflowIndex.builder()
                                                .entityManagerFactory(entityManagerFactory)
                                                .executor(heldBack::add)
                                                .timeout(Duration.ofMillis(100))
                                                .build();
            var write = workflowIndex.initialize(INDEX, Set.of("order-1"));
            assertThatThrownBy(() -> write.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);

            // when - the timed out write gets its turn after all
            heldBack.forEach(Runnable::run);

            // then - it does not write behind the back of the caller, which considers it failed
            assertThat(await(createIndex().load(INDEX, Segment.ROOT_SEGMENT))).isEmpty();
        }

        @Test
        void completesAStartedWriteOnlyOnceItsTransactionEnded() throws InterruptedException {
            // given - a transaction that stalls after it began, until the test lets it proceed
            var began = new CountDownLatch(1);
            var proceed = new CountDownLatch(1);
            var provider = new JpaTransactionalExecutorProvider(entityManagerFactory);
            TransactionalExecutorProvider<EntityManager> stalling = context -> new TransactionalExecutor<>() {
                @Override
                public <R> CompletableFuture<R> apply(ThrowingFunction<EntityManager, R, Exception> function) {
                    return provider.getTransactionalExecutor(null).apply(em -> {
                        began.countDown();
                        assertThat(proceed.await(5, TimeUnit.SECONDS)).isTrue();
                        return function.apply(em);
                    });
                }
            };
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                var workflowIndex = JpaWorkflowIndex.builder()
                                                    .transactionalExecutorProvider(stalling)
                                                        .executor(executor)
                                                    .timeout(Duration.ofMillis(100))
                                                    .build();
                // and - start deadlines that pass only when the test says so
                List<CompletableFuture<Void>> startDeadlines = new CopyOnWriteArrayList<>();
                workflowIndex.startDeadlines = () -> {
                    var startDeadline = new CompletableFuture<Void>();
                    startDeadlines.add(startDeadline);
                    return startDeadline;
                };

                // when - the write began its transaction and stalls, and its start deadline is made to pass, which
                // can no longer fail a write that started
                var write = workflowIndex.initialize(INDEX, Set.of("order-1"));
                assertThat(began.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(startDeadlines).isNotEmpty();
                startDeadlines.forEach(startDeadline -> startDeadline.completeExceptionally(new TimeoutException()));

                // then - the transaction may still commit, so the write is not settled yet
                assertThat(write).isNotDone();
                proceed.countDown();
                await(write);
                assertThat(await(createIndex().load(INDEX, Segment.ROOT_SEGMENT))).hasValue(Set.of("order-1"));
            } finally {
                executor.shutdownNow();
            }
        }

        @Test
        void failsTheFutureWhenTheExecutorRejectsTheWork() {
            // given
            Executor rejecting = task -> {
                throw new RejectedExecutionException("shut down");
            };
            var workflowIndex = JpaWorkflowIndex.builder()
                                                .entityManagerFactory(entityManagerFactory)
                                                .executor(rejecting)
                                                .timeout(Duration.ofSeconds(1))
                                                .build();

            // when
            var load = workflowIndex.load(INDEX, Segment.ROOT_SEGMENT);

            // then
            assertThat(load).isCompletedExceptionally();
        }

        @Test
        void rejectsANonPositiveTimeout() {
            // given
            var builder = JpaWorkflowIndex.builder();

            // when / then
            assertThatThrownBy(() -> builder.timeout(Duration.ZERO)).isInstanceOf(AxonConfigurationException.class);
            assertThatThrownBy(() -> builder.timeout(Duration.ofSeconds(-1)))
                    .isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void requiresATransactionalExecutorProvider() {
            // given
            var builder = JpaWorkflowIndex.builder();

            // when / then
            assertThatThrownBy(builder::build).isInstanceOf(AxonConfigurationException.class);
        }
    }
}
