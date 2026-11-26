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
import io.axoniq.framework.workflow.runtime.execution.LiveWorkflows.Hibernated;
import io.axoniq.framework.workflow.runtime.execution.RunningWorkflowsClaimLoader.ClaimedIndex;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The hibernation of the workflows of a {@link WorkflowEngine}: workflows leave memory while they only wait for an
 * external event or a deadline, and are restored from the event store when that event arrives, that deadline expires,
 * or a cancellation is requested for them.
 * <p>
 * The engine always talks to its hibernation, at every point where hibernation changes what it does, so it never checks
 * whether hibernation is enabled. There are two implementations:
 * <ul>
 *     <li>{@link WorkflowEngineHibernationSupport}, when the workflow module enables hibernation: an active component
 *     that runs passes asking idle workflows to hibernate, keeps the timers of the hibernated workflows, adopts the
 *     hibernated workflows a claim accepts the hints of, and wakes them;</li>
 *     <li>{@link NoHibernation}, otherwise: no workflow ever hibernates, so every live workflow of the engine is in
 *     memory, a claim restores every running workflow, and the steps that only guard against a hibernation, such as
 *     the lock a restore takes, are skipped.</li>
 * </ul>
 * <p>
 * The methods fall into four roles, in this order:
 * <ul>
 *     <li><b>lifecycle and queries</b>, as the engine starts and stops: {@link #isEnabled()},
 *     {@link #hibernatedWorkflowIds()}, {@link #start()}, {@link #stop()};</li>
 *     <li><b>residency guards</b>, as an execution enters memory: whether it {@link #mayHibernate(SegmentClaim) may
 *     hibernate} and is {@link #allow(WorkflowExecution, SegmentClaim) allowed} to, and how a restore is
 *     {@link #restoreGuarded(String, Supplier) guarded} against a hibernation of its workflow and
 *     {@link #restoreOverHibernation(SegmentClaim, String, WorkflowExecution, boolean) takes its place};</li>
 *     <li><b>claim lifecycle hooks</b>, as a claim of a segment restores, catches up, fails or ends:
 *     {@link #withAcceptedHints(ClaimedIndex)}, {@link #adopt(SegmentClaim, Map)}, {@link #arm(Segment)},
 *     {@link #dropOf(SegmentClaim)}, {@link #dropOverlapping(Segment)};</li>
 *     <li><b>wakes</b>, for what a hibernated workflow is woken for:
 *     {@link #wakeForEvent(Hibernated, EventMessage, ProcessingContext)} and
 *     {@link #wakeForCancellation(String, Consumer)}.</li>
 * </ul>
 * The engine calls every role on the one hibernation it holds, so they stay one interface.
 * <p>
 * The engine starts with {@link NoHibernation}, and replaces it once, before it starts, when its module enables
 * hibernation, see {@link WorkflowEngine#enableHibernation}. Public, as the workflow module hands the
 * {@link WorkflowEngine#hibernation() hibernation of its engine} to its {@link WorkflowCancellationService};
 * internal, as only the engine creates it.
 * <p>
 * The {@link LiveWorkflows} registry never calls the hibernation: a {@link LiveWorkflows.Hibernated hibernated} entry
 * is data only. The engine hands the {@link #wakeForEvent(Hibernated, EventMessage, ProcessingContext) wake} of its
 * hibernation to the registry as it delivers an event, which runs it under the lock of the workflow, and the
 * {@link WorkflowCancellationService} has it wake a workflow that a request finds hibernated, see
 * {@link #wakeForCancellation(String, Consumer)}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
public sealed interface Hibernation permits WorkflowEngineHibernationSupport, NoHibernation {

    // Lifecycle and queries.

    /**
     * Returns whether workflows may hibernate. The engine never asks: it exists for the
     * {@link SimpleWorkflowManager}, which reads it through {@link WorkflowEngine#isHibernationEnabled()} to skip the
     * work a query only needs to find hibernated workflows, as without hibernation every live workflow is in memory.
     *
     * @return {@code true} when workflows may hibernate, {@code false} when every live workflow is in memory
     */
    boolean isEnabled();

    /**
     * Returns the identifiers of the workflows that hibernated. They are live, but not in memory.
     *
     * @return an unmodifiable snapshot of the identifiers of the hibernated workflows, empty when no workflow
     * hibernates
     */
    Set<String> hibernatedWorkflowIds();

    /**
     * Starts the hibernation, as its engine starts.
     */
    void start();

    /**
     * Stops the hibernation, as its engine shuts down, and forgets every hibernated workflow.
     */
    void stop();

    // Residency guards: as an execution enters memory.

    /**
     * Returns whether an execution started or restored under the given claim may hibernate. Such an execution gets its
     * events under the lock of its workflow, see {@link LiveWorkflows#deliver}; an execution that never hibernates gets
     * them without that lock.
     *
     * @param claim the claim the execution is started or restored under, or {@code null} outside a segmented processor
     * @return {@code true} when the execution may hibernate
     */
    boolean mayHibernate(@Nullable SegmentClaim claim);

    /**
     * Lets the given execution, which just entered memory under the given claim, hibernate from now on, when it
     * {@link #mayHibernate(SegmentClaim) may}.
     *
     * @param execution the execution that entered memory
     * @param claim     the claim it was started or restored under, or {@code null} outside a segmented processor
     */
    void allow(WorkflowExecution execution, @Nullable SegmentClaim claim);

    /**
     * Runs the given {@code restore}, which brings a restored execution of the given workflow into memory, guarded
     * against the hibernations, wakes and adoptions of that workflow: none of them runs while the restore does.
     *
     * @param workflowId the identifier of the restored workflow
     * @param restore    brings the restored execution into memory
     * @param <T>        the type of the result of the restore
     * @return the result of the restore
     */
    <T> T restoreGuarded(String workflowId, Supplier<T> restore);

    /**
     * Brings the given execution, which the given claim restored, into memory in place of a hibernation of its workflow
     * under another claim, if the workflow is hibernated. Only a claim of an overlapping segment, while a split or a
     * merge is in progress, holds a workflow hibernated that another claim restores. The caller holds the lock of the
     * workflow, see {@link #restoreGuarded(String, Supplier)}, and the lock of the restoring claim while it is current.
     *
     * @param claim        the claim restoring the workflow
     * @param workflowId   the identifier of the restored workflow
     * @param execution    the execution the claim restored
     * @param mayHibernate whether the execution may hibernate
     * @return {@code true} when the execution took the place of a hibernation of the workflow, {@code false} when the
     * workflow is not hibernated, so the caller brings the execution into memory itself
     */
    boolean restoreOverHibernation(SegmentClaim claim,
                                   String workflowId,
                                   WorkflowExecution execution,
                                   boolean mayHibernate);

    // Claim lifecycle hooks: as a claim restores, catches up, fails or ends.

    /**
     * Returns the given running workflows a claim loaded, with only the hibernation hints this hibernation accepts: a
     * claim leaves the workflows whose hint it accepts out of memory, as hibernated workflows, see
     * {@link #adopt(SegmentClaim, Map)}, and restores every other one.
     *
     * @param claimed the running workflows of the claimed segment, with the hibernation hints stored for them
     * @return the claimed workflows, with only the accepted hints left
     */
    ClaimedIndex withAcceptedHints(ClaimedIndex claimed);

    /**
     * Leaves the given workflows, whose hints the given claim accepted, out of memory as hibernated workflows of that
     * claim, instead of restoring them. A workflow that is not adopted, as the claim ended meanwhile or the workflow is
     * live already, gets its hint cleared, and is not restored by this claim.
     *
     * @param claim the claim that accepted the hints
     * @param hints the open waits and nearest deadline of each workflow, by identifier, see
     *              {@link #withAcceptedHints(ClaimedIndex)}
     */
    void adopt(SegmentClaim claim, Map<String, HibernationHint> hints);

    /**
     * Lets the workflows adopted by the claim of the given segment wake for their deadline or a cancellation, as the
     * claim caught up.
     *
     * @param segment the segment whose claim caught up
     */
    void arm(Segment segment);

    /**
     * Forgets the hibernated workflows of the given claim, whose restore failed.
     *
     * @param claim the claim that failed
     */
    void dropOf(SegmentClaim claim);

    /**
     * Forgets the hibernated workflows of the given segment, and of every segment overlapping it, as it is released or
     * claimed. Their durable state is untouched: the next claim of their segment restores them.
     *
     * @param segment the segment that is released or claimed
     */
    void dropOverlapping(Segment segment);

    // Wakes: what a hibernated workflow is woken for.

    /**
     * Wakes the given hibernated workflow for an event, on the thread handling the event, and hands it the event before
     * its body starts. The {@link LiveWorkflows} registry runs this under the lock of the workflow, as it delivers the
     * event, see {@link LiveWorkflows#deliver(String, EventMessage, ProcessingContext, LiveWorkflows.WakeForEvent)}.
     *
     * @param hibernated the hibernation of the workflow the event is for
     * @param event      the event that wakes the workflow
     * @param context    the context of the event
     */
    void wakeForEvent(Hibernated hibernated, EventMessage event, ProcessingContext context);

    /**
     * Wakes the given hibernated workflow for a cancellation request, once the hibernation may wake, and hands the
     * woken execution to the given action before its body starts, the way an event that wakes a workflow is handed to
     * it. The {@link WorkflowCancellationService} calls this for a request that found the workflow hibernated, once it
     * released the lock of the workflow, and queues the request on the woken execution in the given action.
     *
     * @param workflowId  the identifier of the hibernated workflow the request is for
     * @param beforeStart receives the woken execution before its body starts
     * @return a future completing once the wake ran, also when it failed and leaves the workflow hibernated, or once it
     * turned out unnecessary, as the workflow is no longer hibernated
     */
    CompletableFuture<Void> wakeForCancellation(String workflowId, Consumer<WorkflowExecution> beforeStart);
}
