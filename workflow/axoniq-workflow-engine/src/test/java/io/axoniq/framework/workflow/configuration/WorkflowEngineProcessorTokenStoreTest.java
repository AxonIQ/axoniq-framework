/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.configuration.AbstractEventSourcedEntityRepositoryTestBase.TestContext;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a workflow module refuses to start when its engine keeps a workflow index that may survive a restart
 * while the event processor of the module keeps its tokens in memory.
 *
 * @author Allard Buijze
 */
class WorkflowEngineProcessorTokenStoreTest {

    private static final String MODULE = "token-store";

    private final List<AxonConfiguration> configurations = new ArrayList<>();

    @AfterEach
    void tearDown() {
        configurations.forEach(AxonConfiguration::shutdown);
    }

    @Nested
    class WithADurableWorkflowIndex {

        @Test
        void refusesToStartWithAnInMemoryTokenStore() {
            // given
            WorkflowIndex index = durable(WorkflowIndex.class, new InMemoryWorkflowIndex());

            // when / then
            assertThatThrownBy(() -> start(index, new InMemoryTokenStore(), UnaryOperator.identity()))
                    .hasRootCauseInstanceOf(AxonConfigurationException.class)
                    .rootCause()
                    .hasMessageContaining(MODULE)
                    .hasMessageContaining("InMemoryTokenStore");
        }

        @Test
        void refusesToStartWithTheInMemoryTokenStoreTheApplicationFallsBackTo() {
            // given - no token store registered
            WorkflowIndex index = durable(WorkflowIndex.class, new InMemoryWorkflowIndex());

            // when / then
            assertThatThrownBy(() -> start(index, null, UnaryOperator.identity()))
                    .hasRootCauseInstanceOf(AxonConfigurationException.class)
                    .rootCause()
                    .hasMessageContaining(MODULE)
                    .hasMessageContaining("InMemoryTokenStore");
        }

        @Test
        void refusesToStartWithADurableTokenStoreSetOnlyThroughTheProcessingDefaults() {
            // given - no token store registered, a durable one set for every pooled streaming processor
            WorkflowIndex index = durable(WorkflowIndex.class, new InMemoryWorkflowIndex());
            TokenStore tokenStore = durable(TokenStore.class, new InMemoryTokenStore());

            // when / then - the processor of the engine takes the unnamed component, the in-memory fallback
            assertThatThrownBy(() -> start(index, null, UnaryOperator.identity(), d -> d.tokenStore(tokenStore)))
                    .hasRootCauseInstanceOf(AxonConfigurationException.class)
                    .rootCause()
                    .hasMessageContaining(MODULE)
                    .hasMessageContaining("unnamed TokenStore component")
                    .hasMessageContaining("event processing defaults");
        }

        @Test
        void refusesToStartWhenTheProcessorConfigurationSetsAnInMemoryTokenStore() {
            // given - a durable token store for the application, replaced for the processor of the module
            WorkflowIndex index = durable(WorkflowIndex.class, new InMemoryWorkflowIndex());
            TokenStore tokenStore = durable(TokenStore.class, new InMemoryTokenStore());

            // when / then
            assertThatThrownBy(() -> start(index, tokenStore, pc -> pc.tokenStore(new InMemoryTokenStore())))
                    .hasRootCauseInstanceOf(AxonConfigurationException.class)
                    .rootCause()
                    .hasMessageContaining(MODULE);
        }

        @Test
        void startsWithADurableTokenStore() {
            // given
            WorkflowIndex index = durable(WorkflowIndex.class, new InMemoryWorkflowIndex());
            TokenStore tokenStore = durable(TokenStore.class, new InMemoryTokenStore());

            // when / then
            assertThatCode(() -> start(index, tokenStore, UnaryOperator.identity())).doesNotThrowAnyException();
        }
    }

    @Nested
    class WithAJpaWorkflowIndex {

        private final EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory(
                "workflowIndex", Map.of("jakarta.persistence.jdbc.url", "jdbc:hsqldb:mem:" + UUID.randomUUID())
        );

        @AfterEach
        void closeEntityManagerFactory() {
            entityManagerFactory.close();
        }

        @Test
        void refusesToStartWithAnInMemoryTokenStore() {
            // given
            WorkflowIndex index = JpaWorkflowIndex.builder()
                                                  .entityManagerFactory(entityManagerFactory)
                                                  .build();

            // when / then
            assertThatThrownBy(() -> start(index, new InMemoryTokenStore(), UnaryOperator.identity()))
                    .hasRootCauseInstanceOf(AxonConfigurationException.class)
                    .rootCause()
                    .hasMessageContaining(MODULE)
                    .hasMessageContaining(JpaWorkflowIndex.class.getName());
        }
    }

    @Nested
    class WithAnInMemoryTokenStore {

        @Test
        void startsWithAnInMemoryWorkflowIndex() {
            // given - an index that is lost together with the tokens
            WorkflowIndex index = new InMemoryWorkflowIndex();

            // when / then
            assertThatCode(() -> start(index, new InMemoryTokenStore(), UnaryOperator.identity()))
                    .doesNotThrowAnyException();
        }

        @Test
        void startsWithoutAWorkflowIndex() {
            // given
            WorkflowIndex index = WorkflowIndex.none();

            // when / then
            assertThatCode(() -> start(index, new InMemoryTokenStore(), UnaryOperator.identity()))
                    .doesNotThrowAnyException();
        }
    }

    private AxonConfiguration start(WorkflowIndex index,
                                    @Nullable TokenStore tokenStore,
                                    UnaryOperator<PooledStreamingEventProcessorConfiguration> processorConfig) {
        return start(index, tokenStore, processorConfig, UnaryOperator.identity());
    }

    private AxonConfiguration start(WorkflowIndex index,
                                    @Nullable TokenStore tokenStore,
                                    UnaryOperator<PooledStreamingEventProcessorConfiguration> processorConfig,
                                    UnaryOperator<PooledStreamingEventProcessorConfiguration> processingDefaults) {
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> {
                                           })
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .processorConfiguration(pc -> processorConfig.apply(pc.initialSegmentCount(1)))
                                   .contextFactory(c -> TestContext::new);
        var configurer = WorkflowConfigurer.create()
                                           .registerWorkflowIndex(c -> index)
                                           .eventSourcing(es -> es.messaging(m -> m.eventProcessing(
                                                   ep -> ep.pooledStreaming(ps -> ps.defaults(processingDefaults))
                                           )));
        configurer.componentRegistry(cr -> {
            cr.registerComponent(EventStorageEngine.class, cfg -> new InMemoryEventStorageEngine());
            if (tokenStore != null) {
                cr.registerComponent(TokenStore.class, cfg -> tokenStore);
            }
            cr.registerModule(module);
        });
        var configuration = configurer.build();
        configurations.add(configuration);
        configuration.start();
        return configuration;
    }

    /**
     * Returns an implementation of the given {@code type} that delegates every call to the given in-memory
     * {@code delegate}, standing in for an implementation that survives a restart, such as one keeping its state in a
     * database.
     */
    private static <T> T durable(Class<T> type, T delegate) {
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[]{type},
                (proxy, method, args) -> {
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
        ));
    }
}
