/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Workflow events and an in-memory event store for tests of the running-workflows index.
 *
 * @author Allard Buijze
 */
final class LifecycleEventFixtures {

    private LifecycleEventFixtures() {
    }

    /**
     * A workflow lifecycle event of the given instance.
     */
    static EventMessage lifecycleEvent(String workflowId, WorkflowStatus status) {
        return EventTestUtils.asEventMessage("lifecycle")
                             .andMetadata(MetadataUtils.create(workflowId, status));
    }

    /**
     * A step event of the given instance, which changes nothing about whether the instance runs.
     */
    static EventMessage stepEvent(String workflowId) {
        return EventTestUtils.asEventMessage("step")
                             .andMetadata(MetadataUtils.create(workflowId, "step", StepStatus.STARTED));
    }

    /**
     * An event published by the given instance through the publish primitive. It carries a lifecycle status, so only
     * its publish marker can keep it from being read as a lifecycle event.
     */
    static EventMessage publishedEvent(String workflowId, WorkflowStatus status) {
        return EventTestUtils.asEventMessage("published")
                             .andMetadata(MetadataUtils.create(workflowId, status)
                                                       .and(MetadataUtils.METADATA_KEY_STEP_PRIMITIVE,
                                                            MetadataUtils.STEP_PRIMITIVE_PUBLISH));
    }

    /**
     * An event store over the given storage engine, tagging events as the workflow configuration does.
     */
    static EventStore eventStore(EventStorageEngine storageEngine) {
        return new StorageEngineBackedEventStore(storageEngine, new SimpleEventBus(), new WorkflowEventTagResolver());
    }

    /**
     * Appends the given events, tagged as the workflow configuration tags them.
     */
    static void append(EventStorageEngine storageEngine, EventMessage... events) {
        var tagResolver = new WorkflowEventTagResolver();
        List<TaggedEventMessage<?>> tagged = Arrays.stream(events)
                                                   .<TaggedEventMessage<?>>map(event -> new GenericTaggedEventMessage<>(
                                                           event, tagResolver.resolve(event)
                                                   ))
                                                   .toList();
        storageEngine.appendEvents(AppendCondition.none(), null, tagged)
                     .thenCompose(transaction -> commit(transaction))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
    }

    private static <R> CompletableFuture<Void> commit(EventStorageEngine.AppendTransaction<R> transaction) {
        return transaction.commit().thenCompose(transaction::afterCommit).thenAccept(marker -> {
        });
    }

    /**
     * A context handling an event at the given position of the given segment.
     */
    static StubProcessingContext handlingContext(Segment segment, TrackingToken token) {
        var context = new StubProcessingContext();
        context.putResource(Segment.RESOURCE_KEY, segment);
        context.putResource(TrackingToken.RESOURCE_KEY, token);
        return context;
    }
}
