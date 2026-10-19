/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.messaging.eventstreaming.checkpoint.CheckpointTrigger;
import io.axoniq.framework.messaging.eventstreaming.checkpoint.Checkpointing;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * Event handling component handling any event, which is checkpoint-aware.
 * <p>
 * This component always participates in checkpoint coordination. This keeps every handler in the workflow processor
 * checkpoint-aware, preserving the engine's deferred checkpointing behavior.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class CheckpointingEventHandlingComponentHandlingAny extends EventHandlingComponentHandlingAny
        implements EventHandlingComponent, Checkpointing {

    private final Checkpointing checkpointingHandler;
    private final boolean supportsReset;

    /**
     * Constructs the component for a workflow engine.
     *
     * @param workflowEngine       workflow engine to deliver events to
     * @param checkpointingHandler handler to notify when checkpointing is required
     */
    public CheckpointingEventHandlingComponentHandlingAny(WorkflowEngine workflowEngine,
                                                          Checkpointing checkpointingHandler) {
        super(workflowEngine);
        this.supportsReset = false; // don't support reset
        this.checkpointingHandler = requireNonNull(checkpointingHandler, "Checkpointing handler must not be null");
    }

    @Override
    public boolean supportsReset() {
        return supportsReset;
    }

    @Override
    public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
        return checkpointingHandler.onCheckpointAdvanced(segment, requested);
    }

    @Override
    public void onSegmentClaimed(Segment segment,
                                 @Nullable TrackingToken from,
                                 CheckpointTrigger trigger) {

        checkpointingHandler.onSegmentClaimed(segment, from, trigger);
    }

    @Override
    public CompletableFuture<TrackingToken> onSegmentReleased(Segment segment, TrackingToken requested) {
        return checkpointingHandler.onSegmentReleased(segment, requested);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        super.describeTo(descriptor);
        descriptor.describeProperty("checkpointingHandler", checkpointingHandler.getClass());
    }
}
