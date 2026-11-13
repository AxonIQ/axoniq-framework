/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.SegmentChangeListener;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Moves workflow executions with their segment: instances are restored on the node claiming their segment and dropped
 * again when it releases them, so segments migrating between nodes carry their instances along without a restart.
 * <p>
 * A claim runs in two nested units of work: a short-lived sourcing context that loads durable workflow state, and an
 * independent execution context that parents the restored workflow bodies, which outlive the claim callback. See
 * {@link WorkflowEngine#restoreWorkflowsFor(Segment, TrackingToken, ProcessingContext, ProcessingContext)} for why the
 * sourcing context must not be reused for them.
 *
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 5.4.0
 */
class WorkflowSegmentChangeListener implements SegmentChangeListener {

    private final String moduleName;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Supplier<WorkflowEngine> workflowEngine;

    /**
     * Creates a segment change listener moving the given engine's workflow executions with their segment.
     *
     * @param moduleName        name of the workflow module, used to name the claim's units of work
     * @param unitOfWorkFactory factory for the unit of work a claim sources durable workflow state in
     * @param workflowEngine    supplies the engine whose executions follow the segments
     */
    WorkflowSegmentChangeListener(String moduleName,
                                  UnitOfWorkFactory unitOfWorkFactory,
                                  Supplier<WorkflowEngine> workflowEngine) {
        this.moduleName = requireNonNull(moduleName, "The module name must not be null.");
        this.unitOfWorkFactory = requireNonNull(unitOfWorkFactory, "The UnitOfWorkFactory must not be null.");
        this.workflowEngine = requireNonNull(workflowEngine, "The WorkflowEngine supplier must not be null.");
    }

    @Override
    public CompletableFuture<Void> onSegmentClaimed(Segment segment, @Nullable TrackingToken from) {
        return unitOfWorkFactory
                .create(moduleName + "SegmentClaim" + segment.getSegmentId())
                .executeWithResult(sourcingContext -> {
                    var executionUnitOfWork = new SimpleUnitOfWorkFactory(sourcingContext)
                            .create(moduleName + "SegmentExecutionContext" + segment.getSegmentId());
                    return executionUnitOfWork.executeWithResult(
                            executionContext -> workflowEngine.get().restoreWorkflowsFor(
                                    segment, from, sourcingContext, executionContext
                            )
                    );
                });
    }

    @Override
    public CompletableFuture<Void> onSegmentReleased(Segment segment) {
        return workflowEngine.get().releaseWorkflowsFor(segment);
    }
}
