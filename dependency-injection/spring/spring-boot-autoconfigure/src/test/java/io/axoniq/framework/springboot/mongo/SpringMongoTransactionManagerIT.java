/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.springboot.mongo;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating that the {@link SpringMongoTransactionManager} runs the processing lifecycle in the transaction
 * of Spring's {@link MongoTransactionManager} against a real MongoDB instance.
 * <p>
 * Everything that is written in the processing lifecycle has to be committed or rolled back in one go: writes through
 * the session of the processing context, writes through Spring Data's {@link MongoTemplate}, and writes through the
 * {@link SpringMongoTemplate} that do not pass a session at all. Uses a replica set, as multi-document transactions are
 * not supported on a standalone MongoDB instance.
 *
 * @author Steven van Beelen
 */
@Testcontainers
class SpringMongoTransactionManagerIT {

    private static final String MONGO_IMAGE = System.getProperty("axoniq.mongo.image", "mongo:7.0");
    private static final String DATABASE = "transaction-test-db";
    private static final String SPRING_DATA_COLLECTION = "springdata";

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer(MONGO_IMAGE).withReplicaSet();

    private MongoClient independentClient;
    private SpringMongoTemplate axonTemplate;
    private MongoTemplate springDataTemplate;
    private MongoTransactionalExecutorProvider provider;
    private UnitOfWorkFactory unitOfWorkFactory;

    @BeforeEach
    void setUp() {
        MongoDatabaseFactory factory =
                new SimpleMongoClientDatabaseFactory(MONGO_CONTAINER.getReplicaSetUrl(DATABASE));
        axonTemplate = SpringMongoTemplate.builder().factory(factory).build();
        springDataTemplate = new MongoTemplate(factory);
        independentClient = MongoClients.create(MONGO_CONTAINER.getReplicaSetUrl(DATABASE));
        // Collections must exist up front, as MongoDB does not create collections within a transaction
        axonTemplate.deadLetterCollection().drop();
        factory.getMongoDatabase().createCollection("deadletters");
        factory.getMongoDatabase().getCollection(SPRING_DATA_COLLECTION).drop();
        factory.getMongoDatabase().createCollection(SPRING_DATA_COLLECTION);
        provider = new MongoTransactionalExecutorProvider(
                () -> factory.getSession(ClientSessionOptions.builder().build())
        );
        unitOfWorkFactory = new TransactionalUnitOfWorkFactory(
                new SpringMongoTransactionManager(new MongoTransactionManager(factory)),
                new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE)
        );
    }

    @AfterEach
    void tearDown() {
        independentClient.close();
    }

    private MongoCollection<Document> committed(String collection) {
        return independentClient.getDatabase(DATABASE).getCollection(collection);
    }

    private CompletableFuture<Void> insertThroughSession(ProcessingContext context, String id) {
        return provider.getTransactionalExecutor(context)
                       .accept(session -> axonTemplate.deadLetterCollection()
                                                      .insertOne(session, new Document("_id", id)));
    }

    private void insertThroughSpringData(String id) {
        springDataTemplate.insert(new Document("_id", id), SPRING_DATA_COLLECTION);
    }

    private void insertWithoutSession(String id) {
        axonTemplate.deadLetterCollection().insertOne(new Document("_id", id));
    }

    @Nested
    class WhenTheProcessingLifecycleCommits {

        @Test
        void writesOfAllThreeKindsAreCommittedInOneGo() throws Exception {
            // given
            AtomicLong visibleBeforeCommit = new AtomicLong(-1);

            // when
            unitOfWorkFactory.create().executeWithResult(
                    context -> insertThroughSession(context, "session").thenApply(v -> {
                        insertThroughSpringData("spring-data");
                        insertWithoutSession("no-session");
                        visibleBeforeCommit.set(committed("deadletters").countDocuments()
                                                        + committed(SPRING_DATA_COLLECTION).countDocuments());
                        return null;
                    })
            ).get(30, TimeUnit.SECONDS);

            // then
            assertThat(visibleBeforeCommit).hasValue(0);
            assertThat(committed("deadletters").countDocuments()).isEqualTo(2);
            assertThat(committed(SPRING_DATA_COLLECTION).countDocuments()).isEqualTo(1);
        }
    }

    @Nested
    class WhenTheProcessingLifecycleFails {

        @Test
        void writesOfAllThreeKindsAreRolledBackInOneGo() {
            // given / when
            CompletableFuture<Object> result = unitOfWorkFactory.create().executeWithResult(
                    context -> insertThroughSession(context, "session").thenCompose(v -> {
                        insertThroughSpringData("spring-data");
                        insertWithoutSession("no-session");
                        return CompletableFuture.failedFuture(new IllegalStateException("failing on purpose"));
                    })
            );

            // then
            assertThatThrownBy(() -> result.get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(committed("deadletters").countDocuments(Filters.empty())).isZero();
            assertThat(committed(SPRING_DATA_COLLECTION).countDocuments()).isZero();
        }
    }
}
