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
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests, on a real configuration, that an event whose handling failed in the workflow engine is handled again, also
 * when the event processor's error handler swallows the failure instead of releasing the segment.
 *
 * @author Allard Buijze
 */
class WorkflowConfigurerFailedEventTest {

    private static final String MODULE = "failed-event";

    private final EventStorageEngine storageEngine = new InMemoryEventStorageEngine();
    private final TokenStore tokenStore = new InMemoryTokenStore();
    private final AtomicInteger contextCreations = new AtomicInteger();
    private final Set<String> completedBodies = ConcurrentHashMap.newKeySet();
    private AxonConfiguration configuration;

    @BeforeEach
    void setUp() {
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> completedBodies.add(ctx.workflowId()))
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   // An error handler that swallows every failure, so the processor neither retries
                                   // the event nor releases the segment by itself.
                                   .processorConfiguration(pc -> pc.initialSegmentCount(1)
                                                                   .tokenClaimInterval(200)
                                                                   .errorHandler(errorContext -> {
                                                                   }))
                                   .contextFactory(c -> this::createContextFailingOnce);
        configuration = WorkflowConfigurer.create().componentRegistry(cr -> cr
                .registerComponent(EventStorageEngine.class, cfg -> storageEngine)
                .registerComponent(TokenStore.class, cfg -> tokenStore)
                .registerModule(module)
        ).build();
        configuration.start();
    }

    @AfterEach
    void tearDown() {
        configuration.shutdown();
    }

    @Test
    void handlesAFailedEventAgainWhenTheErrorHandlerSwallowsTheFailure() {
        // when - starting the workflow fails the first time
        var workflowId = publishStart();

        // then - the engine asked the processor to release the segment without storing a position past the event, so
        // the next claim handles the event again and starts the workflow
        await().atMost(10, TimeUnit.SECONDS).until(() -> completedBodies.contains(workflowId));
        assertThat(contextCreations.get()).isGreaterThanOrEqualTo(2);
    }

    /**
     * Creates the context of a new workflow, except the first time, the way a context reading an unavailable resource
     * fails.
     */
    private TestContext createContextFailingOnce(Map<String, @Nullable Object> payload,
                                                 String workflowId,
                                                 ProcessingContext processingContext,
                                                 WorkflowConfiguration<?> workflowConfiguration) {
        if (contextCreations.incrementAndGet() == 1) {
            throw new IllegalStateException("The first context creation fails.");
        }
        return new TestContext(payload, workflowId, processingContext, workflowConfiguration);
    }

    /**
     * Publishes a start event and returns its identifier, which is the id of the workflow it starts.
     */
    private String publishStart() {
        var event = new GenericEventMessage(new MessageType("start"), Map.of("orderId", "order-1"));
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-start")
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
        return event.identifier();
    }
}
