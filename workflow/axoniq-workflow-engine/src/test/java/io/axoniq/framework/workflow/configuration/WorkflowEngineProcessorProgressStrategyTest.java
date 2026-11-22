/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.messaging.eventstreaming.checkpoint.Checkpointing;
import io.axoniq.framework.messaging.eventstreaming.checkpoint.CheckpointingProgressStrategyFactory;
import io.axoniq.framework.workflow.configuration.AbstractEventSourcedEntityRepositoryTestBase.TestContext;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.DelegatingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.progress.SegmentProgressStrategyFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that the event processor of a workflow module lets the engine decide when to store its tokens whenever the
 * engine keeps a workflow index.
 *
 * @author Allard Buijze
 */
class WorkflowEngineProcessorProgressStrategyTest {

    private static final String MODULE = "progress-strategy";

    private final List<AxonConfiguration> configurations = new ArrayList<>();

    @AfterEach
    void tearDown() {
        configurations.forEach(AxonConfiguration::shutdown);
    }

    /**
     * Lets the processor see an ordinary event handling component next to the engine, which makes it checkpoint at the
     * end of every batch on its own.
     */
    private static PooledStreamingEventProcessorConfiguration withOrdinaryComponent(
            PooledStreamingEventProcessorConfiguration processorConfig
    ) {
        return processorConfig.progressStrategyFactoryBuilder(components -> {
            List<EventHandlingComponent> mixed = new ArrayList<>(components);
            mixed.add(SimpleEventHandlingComponent.create("ordinary"));
            return CheckpointingProgressStrategyFactory.detecting(processorConfig.processorName(),
                                                                  processorConfig::tokenStore)
                                                       .apply(mixed);
        });
    }

    /**
     * Hides the engine from the processor's checkpointing behind a decorator that does not expose it, next to another
     * component that checkpoints on its own, so the engine is no participant in the processor's checkpoints.
     */
    private static PooledStreamingEventProcessorConfiguration hidingTheEngine(
            PooledStreamingEventProcessorConfiguration processorConfig
    ) {
        return processorConfig.progressStrategyFactoryBuilder(components -> {
            List<EventHandlingComponent> decorated = new ArrayList<>();
            components.forEach(component -> decorated.add(new DelegatingEventHandlingComponent(component) {
                @Override
                public <C> Optional<C> unwrap(Class<C> componentType) {
                    return componentType == Checkpointing.class ? Optional.empty() : super.unwrap(componentType);
                }
            }));
            decorated.add(new SelfCheckpointingComponent());
            return CheckpointingProgressStrategyFactory.detecting(processorConfig.processorName(),
                                                                  processorConfig::tokenStore)
                                                       .apply(decorated);
        });
    }

    private static PooledStreamingEventProcessorConfiguration storingTokensOnItsOwn(
            PooledStreamingEventProcessorConfiguration processorConfig
    ) {
        return processorConfig.progressStrategyFactoryBuilder(components -> SegmentProgressStrategyFactory.tokenStoring());
    }

    @Nested
    class WithAWorkflowIndex {

        @Test
        void refusesToStartAProcessorWithAnOrdinaryEventHandlingComponent() {
            // given - a processor that also checkpoints at the end of every batch for an ordinary component
            UnaryOperator<PooledStreamingEventProcessorConfiguration> processorConfig =
                    WorkflowEngineProcessorProgressStrategyTest::withOrdinaryComponent;

            // when / then
            assertThatThrownBy(() -> start(true, processorConfig))
                    .hasRootCauseInstanceOf(AxonConfigurationException.class)
                    .rootCause()
                    .hasMessageContaining(MODULE);
        }

        @Test
        void refusesToStartAProcessorThatDoesNotCheckpointThroughTheEngine() {
            // given - a processor whose checkpoints do not ask the engine
            UnaryOperator<PooledStreamingEventProcessorConfiguration> processorConfig =
                    WorkflowEngineProcessorProgressStrategyTest::hidingTheEngine;

            // when / then
            assertThatThrownBy(() -> start(true, processorConfig))
                    .hasRootCauseInstanceOf(AxonConfigurationException.class)
                    .rootCause()
                    .hasMessageContaining(MODULE);
        }

        @Test
        void refusesToStartAProcessorThatStoresItsTokensOnItsOwn() {
            // given - a processor that stores its tokens without asking the engine
            UnaryOperator<PooledStreamingEventProcessorConfiguration> processorConfig =
                    WorkflowEngineProcessorProgressStrategyTest::storingTokensOnItsOwn;

            // when / then
            assertThatThrownBy(() -> start(true, processorConfig))
                    .hasRootCauseInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void startsAProcessorWithTheEngineAsItsOnlyComponent() {
            // given - the processor configuration of the module, which has the engine as its only component
            UnaryOperator<PooledStreamingEventProcessorConfiguration> processorConfig = UnaryOperator.identity();

            // when / then
            assertThatCode(() -> start(true, processorConfig)).doesNotThrowAnyException();
        }
    }

    @Nested
    class WithoutAWorkflowIndex {

        @Test
        void startsAProcessorWithAnOrdinaryEventHandlingComponent() {
            // given - a processor that also checkpoints at the end of every batch for an ordinary component
            UnaryOperator<PooledStreamingEventProcessorConfiguration> processorConfig =
                    WorkflowEngineProcessorProgressStrategyTest::withOrdinaryComponent;

            // when / then
            assertThatCode(() -> start(false, processorConfig)).doesNotThrowAnyException();
        }
    }

    private AxonConfiguration start(boolean indexed,
                                    UnaryOperator<PooledStreamingEventProcessorConfiguration> processorConfig) {
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
        var configurer = WorkflowConfigurer.create();
        if (indexed) {
            configurer.registerWorkflowIndex(c -> new InMemoryWorkflowIndex());
        }
        configurer.componentRegistry(cr -> cr
                .registerComponent(EventStorageEngine.class, cfg -> new InMemoryEventStorageEngine())
                .registerComponent(TokenStore.class, cfg -> new InMemoryTokenStore())
                .registerModule(module));
        var configuration = configurer.build();
        configurations.add(configuration);
        configuration.start();
        return configuration;
    }

    /**
     * An event handling component that checkpoints on its own, completing every checkpoint at the requested token.
     */
    private static final class SelfCheckpointingComponent extends DelegatingEventHandlingComponent
            implements Checkpointing {

        private SelfCheckpointingComponent() {
            super(SimpleEventHandlingComponent.create("self-checkpointing"));
        }

        @Override
        public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
            return CompletableFuture.completedFuture(requested);
        }
    }
}
