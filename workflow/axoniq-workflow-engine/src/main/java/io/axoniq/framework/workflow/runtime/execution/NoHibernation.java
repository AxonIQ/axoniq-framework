/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.execution.RunningWorkflowsClaimLoader.ClaimedIndex;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The {@link Hibernation} of an engine whose workflow module does not enable hibernation: no workflow ever hibernates,
 * so every live workflow of the engine is in memory.
 * <p>
 * Where hibernation changes what the engine does, this keeps the behavior of an engine without it:
 * <ul>
 *     <li>a claim restores every running workflow, also one with a hibernation hint, and clears that hint, see
 *     {@link #withAcceptedHints(ClaimedIndex)};</li>
 *     <li>a restore takes no lock of its workflow, as no hibernation, wake or adoption can interleave with it, see
 *     {@link #restoreGuarded(String, Supplier)}, and always brings the restored execution into memory itself, see
 *     {@link #restoreOverHibernation(SegmentClaim, String, WorkflowExecution, boolean)};</li>
 *     <li>an event reaches an execution without the lock of its workflow, as no execution
 *     {@link #mayHibernate(SegmentClaim) may hibernate}.</li>
 * </ul>
 * Internal, as only the engine uses it.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
enum NoHibernation implements Hibernation {

    /**
     * The single instance.
     */
    INSTANCE;

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public Set<String> hibernatedWorkflowIds() {
        return Set.of();
    }

    @Override
    public void start() {
        // No passes: nothing hibernates.
    }

    @Override
    public void stop() {
        // Nothing hibernated to forget.
    }

    @Override
    public boolean mayHibernate(@Nullable SegmentClaim claim) {
        return false;
    }

    @Override
    public void allow(WorkflowExecution execution, @Nullable SegmentClaim claim) {
        // Never hibernates.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Accepts no hint: the claim restores every running workflow, and clears the hints of the hibernated ones.
     */
    @Override
    public ClaimedIndex withAcceptedHints(ClaimedIndex claimed) {
        return claimed.restoringHibernated();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Adopts nothing: the claim gets no hint to adopt, as {@link #withAcceptedHints(ClaimedIndex)} accepts none.
     */
    @Override
    public void adopt(SegmentClaim claim, Map<String, HibernationHint> hints) {
        // Nothing to adopt.
    }

    @Override
    public void arm(Segment segment) {
        // Nothing adopted to arm.
    }

    @Override
    public void dropOf(SegmentClaim claim) {
        // Nothing hibernated to forget.
    }

    @Override
    public void dropOverlapping(Segment segment) {
        // Nothing hibernated to forget.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Runs the restore without the lock of the workflow: no workflow hibernates, wakes or is adopted.
     */
    @Override
    public <T> T restoreGuarded(String workflowId, Supplier<T> restore) {
        return restore.get();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Never restores over a hibernation, as no workflow is hibernated without hibernation.
     */
    @Override
    public boolean restoreOverHibernation(SegmentClaim claim,
                                          String workflowId,
                                          WorkflowExecution execution,
                                          boolean mayHibernate) {
        return false;
    }
}
