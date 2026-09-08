/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.messaging.eventstreaming.checkpoint.CheckpointTrigger;
import io.axoniq.framework.messaging.eventstreaming.checkpoint.Checkpointing;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.sequencing.HierarchicalSequencingPolicy;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.core.sequencing.SequentialPerAggregatePolicy;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * Event handling component handling any event.
 * <p>
 * This component always participates in checkpoint coordination, including when it wraps a plain {@link EventHandler}.
 * A plain handler acknowledges requested checkpoint tokens immediately. This keeps every handler in the workflow
 * processor checkpoint-aware, preserving the engine's deferred checkpointing behavior.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class EventHandlingComponentHandlingAny implements EventHandlingComponent, Checkpointing {

    private static final Logger logger = LoggerFactory.getLogger(EventHandlingComponentHandlingAny.class);

    private static final boolean ANY_EVENT = true;

    private final EventHandler eventHandler;
    private final SequencingPolicy<EventMessage> sequencingPolicy;
    @Nullable
    private final Checkpointing checkpointingHandler;

    /**
     * Constructs the component for a generic event handler.
     *
     * @param eventHandler event handler to wrap
     */
    public EventHandlingComponentHandlingAny(EventHandler eventHandler) {
        this.eventHandler = requireNonNull(eventHandler, "Event handler must not be null");
        this.sequencingPolicy = new HierarchicalSequencingPolicy<>(
                SequentialPerAggregatePolicy.INSTANCE,
                SequentialPolicy.INSTANCE
        );
        this.checkpointingHandler = null;
    }

    /**
     * Constructs the component for a workflow engine.
     *
     * @param workflowEngine             workflow engine to deliver events to
     * @param checkpointingHandler       handler to notify when checkpointing is required
     */
    public EventHandlingComponentHandlingAny(WorkflowEngine workflowEngine,
                                             Checkpointing checkpointingHandler) {
        this.eventHandler = requireNonNull(workflowEngine, "Workflow engine handler must not be null");
        this.sequencingPolicy = new HierarchicalSequencingPolicy<>(
                SequentialPerAggregatePolicy.INSTANCE,
                SequentialPolicy.INSTANCE
        );
        this.checkpointingHandler = requireNonNull(checkpointingHandler, "Checkpointing handler must not be null");
    }

    @Override
    public MessageStream.Empty<Message> handle(EventMessage event, ProcessingContext context) {
        logger.debug("Handling event {}", event);
        return eventHandler.handle(event, context);
    }

    @Override
    public Set<QualifiedName> supportedEvents() {
        return Set.of();
    }

    @Override
    public boolean supports(QualifiedName eventName) {
        return ANY_EVENT;
    }

    @Override
    public Object sequenceIdentifierFor(EventMessage event,
                                        ProcessingContext context) {
        return sequencingPolicy.sequenceIdentifierFor(event, context);
    }

    @Override
    public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment,
                                                                 TrackingToken requested) {
        return checkpointingHandler != null
                ? checkpointingHandler.onCheckpointAdvanced(segment, requested)
                : CompletableFuture.completedFuture(requested);
    }

    @Override
    public void onSegmentClaimed(Segment segment,
                                 @Nullable TrackingToken from,
                                 CheckpointTrigger trigger) {
        if (checkpointingHandler != null) {
            checkpointingHandler.onSegmentClaimed(segment, from, trigger);
        }
    }

    @Override
    public CompletableFuture<TrackingToken> onSegmentReleased(Segment segment,
                                                              TrackingToken requested) {
        if (checkpointingHandler != null) {
            return checkpointingHandler.onSegmentReleased(segment, requested);
        }
        // Plain handlers add no checkpoint work but must acknowledge to preserve deferred checkpoint coordination.
        return CompletableFuture.completedFuture(requested);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("eventHandler", eventHandler.getClass());
        if (checkpointingHandler != null) {
            descriptor.describeProperty("checkpointingHandler", checkpointingHandler.getClass());
        }
    }
}
