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
package io.axoniq.workflow.configuration;

import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.SegmentChangeListener;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Moves workflow executions with their segment: instances are restored on the node claiming their segment and
 * dropped again when it releases them, so segments migrating between nodes carry their instances along without a
 * restart.
 * <p>
 * A claim runs in two nested units of work: a short-lived sourcing context that loads durable workflow state, and an
 * independent execution context that parents the restored workflow bodies, which outlive the claim callback. See
 * {@link WorkflowEngine#restoreWorkflowsFor(Segment, TrackingToken, ProcessingContext, ProcessingContext)} for why the
 * sourcing context must not be reused for them.
 *
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 0.3.0
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
                    return executionUnitOfWork.executeWithResult(executionContext -> {
                        workflowEngine.get().restoreWorkflowsFor(segment, from, sourcingContext, executionContext);
                        return CompletableFuture.<Void>completedFuture(null);
                    });
                });
    }

    @Override
    public CompletableFuture<Void> onSegmentReleased(Segment segment) {
        workflowEngine.get().releaseWorkflowsFor(segment);
        return CompletableFuture.completedFuture(null);
    }
}
