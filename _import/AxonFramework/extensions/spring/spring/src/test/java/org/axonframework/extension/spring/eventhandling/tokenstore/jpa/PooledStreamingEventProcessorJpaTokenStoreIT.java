/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extension.spring.eventhandling.tokenstore.jpa;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.jpa.SimpleEntityManagerProvider;
import org.axonframework.conversion.TestConverter;
import org.axonframework.extension.spring.messaging.unitofwork.SpringTransactionManager;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.TokenEntry;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.hibernate.dialect.HSQLDialect;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.SharedEntityManagerCreator;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * End-to-end regression test for <a href="https://github.com/AxonIQ/AxonFramework/issues/4632">issue #4632</a>.
 * <p>
 * A {@link PooledStreamingEventProcessor} backed by a Spring-managed {@link JpaTokenStore} must initialize its token
 * segments even when the initial token resolves on a foreign thread, as the Axon Server connector does (it completes
 * the token future on a gRPC callback thread). Before the fix the persist ran on that thread, without the thread-bound
 * Spring transaction, and {@code start()} exhausted its retries.
 */
class PooledStreamingEventProcessorJpaTokenStoreIT {

    private static final String PROCESSOR_NAME = "fresh-processor";
    private static final String GRPC_THREAD_NAME = "axon-server-grpc-callback";

    private EntityManagerFactory entityManagerFactory;
    private ExecutorService grpcCallbackExecutor;
    private ScheduledExecutorService coordinatorExecutor;
    private ScheduledExecutorService workerExecutor;
    private PooledStreamingEventProcessor processor;

    @BeforeEach
    void setUp() {
        LocalContainerEntityManagerFactoryBean emfBean = new LocalContainerEntityManagerFactoryBean();
        emfBean.setPersistenceProvider(new HibernatePersistenceProvider());
        emfBean.setPackagesToScan(TokenEntry.class.getPackage().getName());
        Map<String, Object> jpaProperties = new HashMap<>();
        jpaProperties.put("hibernate.dialect", HSQLDialect.class.getName());
        jpaProperties.put("hibernate.hbm2ddl.auto", "create-drop");
        jpaProperties.put("hibernate.show_sql", "false");
        jpaProperties.put("hibernate.connection.url", "jdbc:hsqldb:mem:bug4632-e2e-" + UUID.randomUUID());
        emfBean.setJpaPropertyMap(jpaProperties);
        emfBean.afterPropertiesSet();
        entityManagerFactory = Objects.requireNonNull(emfBean.getObject(), "EntityManagerFactory");

        // Spring's shared EntityManager: a persist requires a transaction bound to the current thread.
        EntityManager sharedEntityManager = SharedEntityManagerCreator.createSharedEntityManager(entityManagerFactory);
        UnitOfWorkFactory unitOfWorkFactory = getUnitOfWorkFactory(sharedEntityManager);

        JpaTokenStore tokenStore = new JpaTokenStore(new JpaTransactionalExecutorProvider(entityManagerFactory),
                                                     TestConverter.JACKSON.getConverter(),
                                                     JpaTokenStoreConfiguration.DEFAULT.nodeId("local"));

        grpcCallbackExecutor = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, GRPC_THREAD_NAME));
        coordinatorExecutor = Executors.newSingleThreadScheduledExecutor(
                runnable -> new Thread(runnable, "coordinator-executor"));
        workerExecutor = Executors.newScheduledThreadPool(2, runnable -> new Thread(runnable, "worker-executor"));

        var configuration = new PooledStreamingEventProcessorConfiguration(
                new EventProcessorConfiguration(PROCESSOR_NAME, null))
                .eventSource(new ForeignThreadInitialTokenEventSource(grpcCallbackExecutor))
                .unitOfWorkFactory(unitOfWorkFactory)
                .tokenStore(tokenStore)
                .coordinatorExecutor(coordinatorExecutor)
                .workerExecutor(workerExecutor)
                .initialSegmentCount(1);

        processor = new PooledStreamingEventProcessor(
                PROCESSOR_NAME,
                List.of(SimpleEventHandlingComponent.create(PROCESSOR_NAME)),
                configuration);
    }

    private @NonNull UnitOfWorkFactory getUnitOfWorkFactory(EntityManager sharedEntityManager) {
        EntityManagerProvider entityManagerProvider = new SimpleEntityManagerProvider(sharedEntityManager);
        JpaTransactionManager platformTransactionManager = new JpaTransactionManager(entityManagerFactory);
        SpringTransactionManager springTransactionManager =
                new SpringTransactionManager(platformTransactionManager, entityManagerProvider, null);
        return new TransactionalUnitOfWorkFactory(
                springTransactionManager, new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE));
    }

    @AfterEach
    void tearDown() {
        if (processor != null) {
            try {
                processor.shutdown().get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // best-effort shutdown
            }
        }
        if (coordinatorExecutor != null) {
            coordinatorExecutor.shutdownNow();
        }
        if (workerExecutor != null) {
            workerExecutor.shutdownNow();
        }
        if (grpcCallbackExecutor != null) {
            grpcCallbackExecutor.shutdownNow();
        }
        if (entityManagerFactory != null && entityManagerFactory.isOpen()) {
            entityManagerFactory.close();
        }
    }

    @Test
    void startsAndInitializesJpaTokenStoreWhenInitialTokenResolvesOnForeignThread() {
        // when starting the processor on a fresh database; the initial token resolves on the gRPC callback thread
        CompletableFuture<Void> started = processor.start();

        // then start completes and the segment is persisted. Before the fix the persist ran on the gRPC thread
        // without a Spring transaction, so start() completed exceptionally with a ProcessRetriesExhaustedException.
        assertThatCode(() -> started.orTimeout(20, TimeUnit.SECONDS).join())
                .doesNotThrowAnyException();
        assertThat(persistedSegmentCount()).isEqualTo(1);
    }

    private long persistedSegmentCount() {
        try (EntityManager entityManager = entityManagerFactory.createEntityManager()) {
            entityManager.getTransaction().begin();
            Long count = entityManager.createQuery(
                                              "SELECT COUNT(t) FROM TokenEntry t WHERE t.processorName = :processorName",
                                              Long.class)
                                      .setParameter("processorName", PROCESSOR_NAME)
                                      .getSingleResult();
            entityManager.getTransaction().commit();
            return count;
        }
    }

    /**
     * A {@link StreamableEventSource} that completes its token futures on a foreign thread, mimicking the Axon Server
     * connector's gRPC callbacks. The delay ensures the token-consuming continuation runs on that thread. The event
     * stream is empty; this test only exercises token-store initialization.
     */
    @NullMarked
    private record ForeignThreadInitialTokenEventSource(Executor grpcCallbackExecutor) implements StreamableEventSource {

        private CompletableFuture<TrackingToken> resolvedOnCallbackThread() {
            return CompletableFuture.supplyAsync(
                    () -> new GlobalSequenceTrackingToken(-1),
                    CompletableFuture.delayedExecutor(50, TimeUnit.MILLISECONDS, grpcCallbackExecutor));
        }

        @Override
        public CompletableFuture<TrackingToken> firstToken(@Nullable ProcessingContext context) {
            return resolvedOnCallbackThread();
        }

        @Override
        public CompletableFuture<TrackingToken> latestToken(@Nullable ProcessingContext context) {
            return resolvedOnCallbackThread();
        }

        @Override
        public CompletableFuture<TrackingToken> tokenAt(Instant at, @Nullable ProcessingContext context) {
            return resolvedOnCallbackThread();
        }

        @Override
        public MessageStream<EventMessage> open(StreamingCondition condition, @Nullable ProcessingContext context) {
            return MessageStream.empty();
        }
    }
}
