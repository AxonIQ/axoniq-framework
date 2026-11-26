/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
/**
 * Internal implementation of the workflow execution engine: the {@code WorkflowEngine}, its scheduling, state and
 * storage, checkpointing/replay support, cancellation coordination, and the delegates implementing individual step
 * primitives.
 *
 * <h2>Live workflows</h2>
 * A workflow is live on this node while its execution is in memory, or while it is hibernated. Four classes share the
 * work:
 * <ul>
 *     <li>{@link LiveWorkflows} holds one entry per live workflow, and applies every move between memory and
 *     hibernation as a compare-and-set on that entry;</li>
 *     <li>{@link SegmentClaims} holds the current {@link SegmentClaim claim} of each segment, which tells when the
 *     bodies of the workflows of that segment start;</li>
 *     <li>the {@link Hibernation} of the {@link WorkflowEngine} decides when a workflow hibernates, and runs its
 *     wakes;</li>
 *     <li>the {@link WorkflowCancellationService} routes a cancellation request to its workflow, in memory or
 *     hibernated.</li>
 * </ul>
 *
 * <h2>Lock order</h2>
 * Where several of these locks are held, they are taken in this order, never the other way:
 * <ol>
 *     <li><b>workflow</b>: the lock of a workflow, see {@link LiveWorkflows#lock(String)}. It serializes the decisions
 *     about one workflow: starting, adopting, hibernating, waking and dropping it, delivering an event to it when it
 *     may hibernate or is hibernated, and routing a cancellation request to it.</li>
 *     <li><b>claim</b>: the lock of a {@link SegmentClaim}. A workflow never enters memory, hibernates or wakes under a
 *     claim that ended, and the body of an execution never starts both by the activation of its claim and by another
 *     start.</li>
 *     <li><b>swap</b>: the write lock of the swap lock of the {@link BroadcastAddressees}, held by every move between
 *     memory and hibernation, so the lookup of the addressees of a broadcast event misses no workflow that moves.</li>
 *     <li><b>bin</b>: the lock of a bin of the entries of {@link LiveWorkflows}, held by every change of an
 *     entry.</li>
 * </ol>
 * Two rules keep this order:
 * <ul>
 *     <li><b>The swap lock and the bins are leaves.</b> Under the swap lock, only the entries, the rows of the open
 *     waits and the repository are touched; under the lock of a bin, only the factory of an entering execution and the
 *     repository run. Neither takes the lock of a workflow, of a claim or of the
 *     {@link RunningWorkflowsProjection projection}, nor runs or loads a workflow.</li>
 *     <li><b>Leaving memory takes no lock of a workflow.</b> An execution leaves memory on threads that hold the lock
 *     of a claim, such as the cleanup of a failed claim, and the activation of a claim that removes its terminal
 *     executions; a compare-and-remove on its entry makes that lock unnecessary. In turn, an action of a claim never
 *     takes the lock of a workflow: the hibernated workflows of a failed claim are dropped outside its lock, and the
 *     cancellation requests queued on an execution that leaves memory are answered on another thread. As the lock of
 *     its workflow does not keep an execution from leaving, a cancellation request made against it checks afterwards
 *     whether it left, see {@link CancellationRoute}.</li>
 * </ul>
 *
 * <h2>Flows</h2>
 * The paths through these classes, with the locks each step holds.
 *
 * <h3>An event for a hibernated workflow</h3>
 * <ol>
 *     <li>The {@link WorkflowEngine} routes an event that names its workflow to
 *     {@link LiveWorkflows#deliver LiveWorkflows.deliver}, which finds the entry hibernated under the lock of the
 *     workflow, and hands it to {@link Hibernation#wakeForEvent Hibernation.wakeForEvent}.</li>
 *     <li>The {@link WorkflowEngineHibernationSupport} wakes it on the thread handling the event: the
 *     {@link WorkflowRestorer} sources it from the event store within the wake timeout, and, while its claim is
 *     current, {@link LiveWorkflows#wake LiveWorkflows.wake} swaps the restored execution in for the hibernation
 *     (claim, swap, bin), with a {@link CancellationRoute#queued queued cancellation route}.</li>
 *     <li>The execution gets the event, and the {@link WorkflowStarter} starts its body when the claim says bodies
 *     start now; otherwise the activation of the claim starts it.</li>
 *     <li>When that fails, the execution is marked failed to start, and swaps back for its hibernation while its claim
 *     is current, or else leaves memory. The event fails, so the segment is released, and its next claim handles the
 *     event again.</li>
 * </ol>
 *
 * <h3>A broadcast event</h3>
 * <ol>
 *     <li>The {@link WorkflowEngine} hands an event that names no workflow to
 *     {@link LiveWorkflows#broadcast LiveWorkflows.broadcast}.</li>
 *     <li>{@link BroadcastAddressees#of BroadcastAddressees.of} lists the executions of the segment in memory and
 *     matches the open waits of its hibernated workflows, on an optimistic stamp of the swap lock, and once more under
 *     its read lock when a swap ran meanwhile: the addressees as at one instant between swaps.</li>
 *     <li>Each addressee gets the event as for an event of its own: in memory, it handles the event; hibernated, it
 *     wakes as above, but only when its open waits still match the event.</li>
 * </ol>
 *
 * <h3>A cancellation request for a hibernated workflow</h3>
 * <ol>
 *     <li>The {@link WorkflowCancellationService} finds the entry hibernated under the lock of the workflow, and
 *     releases that lock.</li>
 *     <li>{@link Hibernation#wakeForCancellation Hibernation.wakeForCancellation} wakes the workflow as for an event,
 *     but on the wake executor and once its claim caught up, under the lock of the workflow, and swaps it in (claim,
 *     swap, bin). Still under the lock of the workflow, it hands the woken execution to the service before its body
 *     starts, which queues the request on its {@link CancellationRoute#queued queued route}.</li>
 *     <li>The body takes the request as a task once it waits again where it hibernated, and makes it. When the
 *     execution leaves memory first, the route makes the request again, wherever the workflow is then, or, when the
 *     start of the woken execution failed, fails it with that failure; when the wake did not queue it,
 *     {@link QueuedRequest#answerUnqueued QueuedRequest.answerUnqueued} answers it.</li>
 * </ol>
 *
 * <h3>A segment claim: restore and catch-up</h3>
 * <ol>
 *     <li>{@link WorkflowEngine#restoreWorkflowsFor WorkflowEngine.restoreWorkflowsFor} starts a claim, ending the
 *     previous claim of the segment, then stops the executions of the segment, and drops the hibernated workflows of
 *     every segment overlapping it.</li>
 *     <li>The {@link RunningWorkflowsClaimLoader} loads the running workflows of the segment with their hibernation
 *     hints, of which the hibernation keeps those it accepts.</li>
 *     <li>The {@link WorkflowRestorer} sources each workflow without an accepted hint. It enters memory under the lock
 *     of the workflow, when hibernation is enabled, and while the claim is current (claim, bin), or takes the place of
 *     a hibernation of the workflow under an overlapping claim (claim, swap, bin). The workflows with an accepted hint
 *     are adopted as hibernated workflows of the claim instead, only once every restore finished and the claim loaded
 *     in time (workflow, claim, bin).</li>
 *     <li>{@link SegmentClaims} activates the claim once the position of its segment covers the startup head: right
 *     away, on an event the segment handles, or by polling. The activation runs once, under the lock of the claim: it
 *     arms the adopted workflows, removes the terminal executions, and starts the bodies of the others.</li>
 *     <li>A failed restore fails the claim: its executions are stopped under its lock, its hibernated workflows dropped
 *     outside it, and the segment released.</li>
 * </ol>
 *
 * <h3>A workflow hibernates</h3>
 * <ol>
 *     <li>A pass of the {@link WorkflowEngineHibernationSupport} asks each execution in memory that made no progress
 *     for the idle delay to hibernate.</li>
 *     <li>At its next safe point, the execution calls back on its own thread. Under the lock of the workflow, the
 *     support reads its open waits; while the claim is current, it records the hint for the workflow index, and has
 *     {@link LiveWorkflows#hibernate LiveWorkflows.hibernate} swap the execution for a hibernation (claim, swap,
 *     bin).</li>
 *     <li>The execution leaves its entry, its safe point and its cancellation route behind, and the timer of the
 *     hibernation wakes the workflow at its nearest deadline.</li>
 * </ol>
 *
 * <h3>A manager query that names no workflow</h3>
 * <ol>
 *     <li>The {@link SimpleWorkflowManager} collects what it finds for the query. Without a projected history, it
 *     visits every entry of {@link LiveWorkflows} once, without a lock.</li>
 *     <li>With a projected history, it lists the executions in memory that match the query, reads the entry of each
 *     workflow the history finds for it, and, with hibernation, lists the executions in memory once more, which finds
 *     a workflow that woke meanwhile.</li>
 *     <li>A workflow found in memory answers with its state; every other one is sourced from the
 *     {@link WorkflowStore}.</li>
 * </ol>
 *
 * @since 5.4.0
 */
@NullMarked
package io.axoniq.framework.workflow.runtime.execution;

import org.jspecify.annotations.NullMarked;
