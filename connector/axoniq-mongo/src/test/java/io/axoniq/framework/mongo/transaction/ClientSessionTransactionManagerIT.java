/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.transaction;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.axoniq.framework.mongo.util.MongoTestImage;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link ClientSessionTransactionManager} together with the
 * {@link MongoTransactionalExecutorProvider}. Requires a MongoDB replica set, as multi-document transactions are not
 * supported otherwise.
 *
 * @author Steven van Beelen
 */
@Testcontainers
class ClientSessionTransactionManagerIT {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer(MongoTestImage.NAME).withReplicaSet();

    private static final String DATABASE = "transaction-manager-test";

    private final AtomicInteger startedSessions = new AtomicInteger();

    private MongoClient mongoClient;
    private MongoCollection<Document> collection;
    private MongoTransactionalExecutorProvider provider;
    private ClientSessionTransactionManager transactionManager;
    private UnitOfWorkFactory unitOfWorkFactory;

    @BeforeEach
    void setUp() {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getReplicaSetUrl(DATABASE));
        collection = mongoClient.getDatabase(DATABASE).getCollection("documents");
        collection.drop();
        mongoClient.getDatabase(DATABASE).createCollection("documents");
        provider = new MongoTransactionalExecutorProvider(mongoClient);
        transactionManager = new ClientSessionTransactionManager(() -> {
            startedSessions.incrementAndGet();
            return mongoClient.startSession();
        });
        unitOfWorkFactory = new TransactionalUnitOfWorkFactory(
                transactionManager, new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE)
        );
    }

    @AfterEach
    void tearDown() {
        mongoClient.close();
    }

    private CompletableFuture<Void> insert(ProcessingContext context, String id) {
        return provider.getTransactionalExecutor(context)
                       .accept(session -> collection.insertOne(session, new Document("_id", id)));
    }

    @Nested
    class WhenStartingATransactionDirectly {

        @Test
        void writesThroughTheSessionAreVisibleOnlyAfterTheCommit() {
            // given
            ClientSessionTransaction transaction = transactionManager.startTransaction();

            // when
            collection.insertOne(transaction.session(), new Document("_id", "direct"));

            // then
            assertThat(collection.countDocuments()).isZero();
            transaction.commit();
            assertThat(collection.countDocuments(eq("_id", "direct"))).isEqualTo(1);
        }

        @Test
        void writesThroughTheSessionAreDiscardedOnRollback() {
            // given
            ClientSessionTransaction transaction = transactionManager.startTransaction();
            collection.insertOne(transaction.session(), new Document("_id", "discarded"));

            // when
            transaction.rollback();

            // then
            assertThat(collection.countDocuments()).isZero();
        }

        @Test
        void executorHandsOutTheSessionOfTheTransaction() throws Exception {
            // given
            ClientSessionTransaction transaction = transactionManager.startTransaction();

            // when
            Boolean sameSessionWithActiveTransaction = transaction.executor().apply(
                    session -> session == transaction.session() && session.hasActiveTransaction()
            ).get(30, TimeUnit.SECONDS);
            transaction.rollback();

            // then
            assertThat(sameSessionWithActiveTransaction).isTrue();
        }

        @Test
        void aTransactionIsConcludedOnce() {
            // given
            ClientSessionTransaction transaction = transactionManager.startTransaction();
            collection.insertOne(transaction.session(), new Document("_id", "kept"));
            transaction.commit();

            // when
            transaction.rollback();
            transaction.commit();

            // then
            assertThat(collection.countDocuments(eq("_id", "kept"))).isEqualTo(1);
        }

        @Test
        void executeInTransactionRunsTheTaskAndCommits() {
            // given
            AtomicInteger runs = new AtomicInteger();

            // when
            transactionManager.executeInTransaction(runs::incrementAndGet);

            // then
            assertThat(runs).hasValue(1);
            assertThat(startedSessions).hasValue(1);
        }

        @Test
        void executeInTransactionRethrowsTheFailureOfTheTask() {
            // given / when / then
            assertThatThrownBy(() -> transactionManager.executeInTransaction(() -> {
                throw new IllegalStateException("task failed");
            })).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class WhenTheProcessingLifecycleCommits {

        @Test
        void writesAreVisibleOnlyAfterTheCommit() throws Exception {
            // given / when
            AtomicInteger visibleBeforeCommit = new AtomicInteger(-1);
            unitOfWorkFactory.create().executeWithResult(context -> insert(context, "committed").thenApply(v -> {
                visibleBeforeCommit.set((int) collection.countDocuments(eq("_id", "committed")));
                return null;
            })).get(30, TimeUnit.SECONDS);

            // then
            assertThat(visibleBeforeCommit).hasValue(0);
            assertThat(collection.countDocuments(eq("_id", "committed"))).isEqualTo(1);
        }

        @Test
        void allWritesThroughTheSessionAreCommittedInOneGo() throws Exception {
            // given / when
            unitOfWorkFactory.create().executeWithResult(
                    context -> insert(context, "one").thenCompose(v -> insert(context, "two"))
            ).get(30, TimeUnit.SECONDS);

            // then
            assertThat(collection.countDocuments()).isEqualTo(2);
        }

        @Test
        void executorProvidesASessionWithAnActiveTransaction() throws Exception {
            // given / when
            Boolean active = unitOfWorkFactory.create().executeWithResult(
                    context -> provider.getTransactionalExecutor(context)
                                       .apply(ClientSession::hasActiveTransaction)
            ).get(30, TimeUnit.SECONDS);

            // then
            assertThat(active).isTrue();
        }

        @Test
        void noSessionIsStartedWhenTheExecutorIsNotUsed() throws Exception {
            // given / when
            unitOfWorkFactory.create().executeWithResult(context -> CompletableFuture.completedFuture("done"))
                             .get(30, TimeUnit.SECONDS);

            // then
            assertThat(startedSessions).hasValue(0);
        }
    }

    @Nested
    class WhenTheProcessingLifecycleFails {

        @Test
        void writesAreRolledBack() {
            // given / when
            CompletableFuture<Object> result = unitOfWorkFactory.create().executeWithResult(
                    context -> insert(context, "rolled-back").thenCompose(v -> CompletableFuture.failedFuture(
                            new IllegalStateException("Handling failed after the write")
                    ))
            );

            // then
            assertThatThrownBy(() -> result.get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(collection.countDocuments()).isZero();
        }

        @Test
        void aFailedWriteRollsBackTheEarlierWritesOfTheSameLifecycle() {
            // given
            collection.insertOne(new Document("_id", "existing"));

            // when
            CompletableFuture<Void> result = unitOfWorkFactory.create().executeWithResult(
                    context -> insert(context, "new").thenCompose(v -> insert(context, "existing"))
            );

            // then
            assertThatThrownBy(() -> result.join()).isInstanceOf(CompletionException.class);
            assertThat(collection.countDocuments(eq("_id", "new"))).isZero();
        }
    }
}
