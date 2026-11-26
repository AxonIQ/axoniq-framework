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
import io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndexConfiguration;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.progress.SegmentProgressStrategyFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests that a workflow module with a {@link WorkflowIndex} refuses to start when its processor could store a token
 * past row changes the index does not hold yet.
 *
 * @author Stefan Dragisic
 */
class SimpleWorkflowModuleWorkflowIndexStartTest {

    private static final String MODULE = "orders";

    private final List<AxonConfiguration> configurations = new ArrayList<>();
    private EntityManagerFactory entityManagerFactory;

    @BeforeEach
    void setUp() {
        entityManagerFactory = Persistence.createEntityManagerFactory("workflow-index");
    }

    @AfterEach
    void tearDown() {
        configurations.forEach(AxonConfiguration::shutdown);
        entityManagerFactory.close();
    }

    @Test
    void startsWithAnInMemoryIndexAndAnInMemoryTokenStore() {
        assertThatCode(() -> start(new InMemoryWorkflowIndex(), UnaryOperator.identity()))
                .doesNotThrowAnyException();
    }

    @Test
    void refusesAnIndexThatSurvivesARestartWhileTheTokensAreKeptInMemory() {
        // given
        var index = new JpaWorkflowIndex(new JpaTransactionalExecutorProvider(entityManagerFactory),
                                         new JacksonConverter(),
                                         JpaWorkflowIndexConfiguration.DEFAULT);

        // when / then
        assertThatThrownBy(() -> start(index, UnaryOperator.identity()))
                .satisfies(failure -> assertThat(rootCauseOf(failure))
                        .isInstanceOf(AxonConfigurationException.class)
                        .hasMessageContaining("InMemoryTokenStore"));
    }

    @Test
    void refusesAProcessorThatStoresItsTokensInAnotherWay() {
        // given
        UnaryOperator<PooledStreamingEventProcessorConfiguration> storingTokensOnItsOwn =
                processor -> processor.progressStrategyFactoryBuilder(
                        components -> SegmentProgressStrategyFactory.tokenStoring()
                );

        // when / then
        assertThatThrownBy(() -> start(new InMemoryWorkflowIndex(), storingTokensOnItsOwn))
                .satisfies(failure -> assertThat(rootCauseOf(failure))
                        .isInstanceOf(AxonConfigurationException.class)
                        .hasMessageContaining("decide when to store its tokens"));
    }

    private void start(WorkflowIndex index, UnaryOperator<PooledStreamingEventProcessorConfiguration> processor) {
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d.declarative(c -> ctx -> {
                                                     })
                                                     .workflowName(MODULE)
                                                     .on(c -> EventConditions.fromQualifiedName(
                                                             new QualifiedName("start")
                                                     ))
                                                     .notCustomized())
                                   .processorConfiguration(processor::apply)
                                   .contextFactory(c -> TestContext::new);
        var configurer = WorkflowConfigurer.create().registerWorkflowIndex(config -> index);
        configurer.componentRegistry(cr -> cr
                .registerComponent(EventStorageEngine.class, cfg -> new InMemoryEventStorageEngine())
                .registerComponent(TokenStore.class, cfg -> new InMemoryTokenStore())
                .registerModule(module));
        var configuration = configurer.build();
        configuration.start();
        configurations.add(configuration);
    }

    private static Throwable rootCauseOf(Throwable failure) {
        var cause = failure;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
