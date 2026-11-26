/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.execution.SegmentClaim.BodyStart;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState.PAYLOAD_TYPE;
import static io.axoniq.framework.workflow.runtime.execution.NewWorkflowInstanceRouting.*;
import static java.util.Objects.requireNonNull;

/**
 * Main workflow component responsible for managing and executing workflows.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 5.4.0
 */
@Internal
public class WorkflowEngine implements
        EventHandler,
        WorkflowEngineCheckpointingSupport.CheckpointLatchCoordinator {

    /**
     * Time {@link #restoreWorkflowsFor(Segment, TrackingToken, ProcessingContext, ProcessingContext)} is given to load
     * a segment's durable state before the restore is considered failed.
     * <p>
     * The workflow store is remote, so the load can stall without failing. The processor only logs a failing claim
     * listener and keeps processing the segment, so a restore that never completes, or fails, would leave the segment
     * claimed here with none of its instances running, and their events would be ignored. A failed restore therefore
     * leaves the claim inert: every event the segment handles fails, so the processor checkpoints none of them and
     * releases the segment, and the claim neither requests a checkpoint nor reports a position on release. It also asks
     * the processor to release the segment through the {@link #setSegmentReleaser(Consumer) segment releaser}, for a
     * segment that handles no event. A later claim, on this node or another, then restores the segment again. A release
     * needs no such bound: the processor already caps it at its claim extension threshold.
     */
    static final Duration DEFAULT_RESTORE_TIMEOUT = Duration.ofSeconds(30);
    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngine.class);
    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private final LiveWorkflows liveWorkflows;
    private final WorkflowRestorer restorer;
    private final WorkflowStarter starter;
    // Package-private so a test can start a new claim of a segment without stopping the executions of the current one,
    // and run the catch-up polls itself.
    final SegmentClaims segmentClaims = new SegmentClaims();
    // The activation this engine hands to every call of its segment claims that may activate a claim. A field, so
    // handling an event does not create a method reference for it.
    private final SegmentClaims.Activation activation = this::removeTerminalAndStartRestoredWorkflowExecutions;
    private final WorkflowEngineSequencingPolicy segmentedRouting;
    private final RunningWorkflowsProjection runningWorkflows;
    private final RunningWorkflowsClaimLoader claimLoader;
    // Package-private so a test can shrink it instead of waiting out the production timeout.
    Duration restoreTimeout = DEFAULT_RESTORE_TIMEOUT;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;
    private volatile Consumer<Segment> segmentReleaser = segment -> {
    };
    // Set once, by enableHibernation, before the engine starts; volatile, as the manager and the cancellation service
    // of the module read it on the threads of their queries and requests.
    private volatile Hibernation hibernation = NoHibernation.INSTANCE;
    // Wakes a hibernated workflow an event is routed to, through the hibernation of this engine. A field, so routing an
    // event does not create a method reference for it.
    private final LiveWorkflows.WakeForEvent wakeForEvent =
            (hibernated, event, context) -> hibernation.wakeForEvent(hibernated, event, context);
    // Set by start, so hibernation cannot be enabled once a claim may have restored workflows without it. Guarded by
    // the monitor of this engine, as start and enableHibernation are synchronized, so the check and the act of each
    // run as one.
    private boolean started;

    /**
     * Creates a new workflow engine keeping its index of running workflows in the given {@code workflowIndex}.
     * Be sure to follow-up construction of a {@code WorkflowEngine} with an invocation of
     * {@link #setCheckpointingSupport(WorkflowEngineCheckpointingSupport)}, as otherwise checkpointing support is
     * unavailable.
     * <p>
     * The engine maintains, per claimed segment, which workflow instances are running, and writes the changes to the
     * {@code workflowIndex} with every checkpoint of the segment, so a claim restores the instances of the stored index
     * plus those started after the token the segment resumes from, instead of reading every lifecycle event ever.
     *
     * @param workflowConfigurationRegistry registry containing a
     *                                      {@link
     *                                      io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration
     *                                      workflow configuration}
     * @param liveWorkflows                 the workflows live in memory, whose residency only the engine changes,
     *                                      and which the cancellation requests of the workflow module find
     * @param workflowStore                 dedicated store for workflow information, like {@link RunningWorkflows} and
     *                                      {@link WorkflowState}
     * @param workflowIndex                 the index persisting the running workflows per segment
     * @param indexName                     the name scoping this engine's entries in the {@code workflowIndex},
     *                                      unique per engine sharing the index
     * @param unitOfWorkFactory             creates the unit of work each restored workflow is sourced in, see
     *                                      {@link DefaultWorkflowRestorer}
     */
    @Internal
    public WorkflowEngine(
            WorkflowConfigurationRegistry<?> workflowConfigurationRegistry,
            LiveWorkflows liveWorkflows,
            WorkflowStore workflowStore,
            WorkflowIndex workflowIndex,
            String indexName,
            UnitOfWorkFactory unitOfWorkFactory
    ) {
        this.workflowConfigurationRegistry = requireNonNull(
                workflowConfigurationRegistry, "The WorkflowConfigurationRegistry must not be null."
        );
        this.liveWorkflows = requireNonNull(liveWorkflows, "The LiveWorkflows must not be null.");
        this.restorer = new DefaultWorkflowRestorer(workflowConfigurationRegistry, workflowStore, unitOfWorkFactory);
        this.starter = new WorkflowStarter(liveWorkflows);
        this.segmentedRouting = new WorkflowEngineSequencingPolicy(workflowConfigurationRegistry);
        this.runningWorkflows = new RunningWorkflowsProjection(workflowIndex, indexName);
        this.claimLoader = new RunningWorkflowsClaimLoader(workflowIndex, indexName, workflowStore);
        EntitlementManager.INSTANCE.registerAddon(WorkflowAxoniqAddon.class);
    }

    /**
     * Sets the checkpointing support required by this engine.
     * <p>
     * The component is set outside the
     * {@link #WorkflowEngine(WorkflowConfigurationRegistry, LiveWorkflows, WorkflowStore, WorkflowIndex, String,
     * UnitOfWorkFactory)}, because they require <b>this</b>
     * {@code WorkflowEngine} itself to function. Hence, a cyclic dependency would exist upon start-up if completion
     * otherwise.
     *
     * @param checkpointingSupport provides checkpointing support to this {@code WorkflowEngine}
     */
    @Internal
    public void setCheckpointingSupport(WorkflowEngineCheckpointingSupport checkpointingSupport) {
        this.checkpointingSupport = requireNonNull(
                checkpointingSupport, "The WorkflowEngineCheckpointingSupport must not be null."
        );
    }

    /**
     * Returns the checkpointing support of this engine, so a test can request a checkpoint of a segment at a given
     * token.
     *
     * @return the checkpointing support set through
     * {@link #setCheckpointingSupport(WorkflowEngineCheckpointingSupport)}
     */
    WorkflowEngineCheckpointingSupport checkpointingSupport() {
        return checkpointingSupport;
    }

    /**
     * Sets the source of the position each claimed segment has consumed up to, including events the processor skipped
     * because they belong to other segments.
     * <p>
     * A segment claimed behind the startup head starts its restored workflows once it caught up with that head. Without
     * a source of positions, it only learns so from an event it handles at or after the head, which a segment whose
     * backlog holds only events of other segments never gets.
     *
     * @param segmentPositions returns the position the given segment consumed up to, or empty when unknown
     */
    @Internal
    public void setSegmentPositions(Function<Segment, Optional<TrackingToken>> segmentPositions) {
        segmentClaims.setSegmentPositions(segmentPositions);
    }

    /**
     * Sets how this engine asks the processor to release a segment whose claim could not restore its workflow
     * executions, or that failed to handle an event.
     * <p>
     * The processor keeps a segment whose claim listener failed. The engine fails every event such a segment handles,
     * which makes the processor release it, but a segment that handles no event is only released through this
     * releaser. Without a releaser, such a segment stays claimed until it handles an event. Likewise, an error handler
     * that does not propagate the failure of an event keeps the segment claimed, while the engine stores no position
     * past that event until the segment is released and claimed again.
     *
     * @param segmentReleaser asks the processor to release the given segment
     */
    @Internal
    public void setSegmentReleaser(Consumer<Segment> segmentReleaser) {
        this.segmentReleaser = requireNonNull(segmentReleaser, "The segment releaser must not be null.");
    }

    /**
     * Enables hibernation: a workflow that only waits for an external event or a deadline is removed from memory once
     * it made no progress for the given idle delay, and restored from the event store when that event arrives, that
     * deadline expires, or a cancellation is requested for it.
     * <p>
     * Set outside the constructor, like {@link #setCheckpointingSupport(WorkflowEngineCheckpointingSupport)}, by the
     * workflow module when hibernation is enabled for it, before this engine {@link #start(TrackingToken) starts}.
     * Hibernation writes no event. A hibernating workflow records its open waits and nearest deadline as a hint in the
     * workflow index, with a checkpoint whose write joins the transaction of the event processor, see
     * {@link WorkflowIndex#storeWithin(String, WorkflowIndex.IndexDelta, ProcessingContext)}. A claim of its segment,
     * here or on another node, leaves a workflow with such a hint out of memory as long as no event of that workflow
     * followed the token the claim resumes from, and restores every other running workflow.
     *
     * @param scheduler    schedules the passes and the deadlines of hibernated workflows
     * @param clock        the clock the scheduler measures deadlines and idle time with
     * @param idleDelay    the time a workflow must make no progress before it hibernates
     * @param wakeTimeout  the time a wake may take to restore a workflow: to source it from the event store and to
     *                     create its execution
     * @param wakeExecutor runs the wakes for a deadline or a cancellation
     * @throws IllegalStateException when hibernation is enabled already, or this engine started
     */
    @Internal
    public synchronized void enableHibernation(WorkflowScheduler scheduler,
                                               Clock clock,
                                               Duration idleDelay,
                                               Duration wakeTimeout,
                                               Executor wakeExecutor) {
        requireNonNull(wakeTimeout, "The wake timeout must not be null.");
        // Once: every hibernated workflow wakes through the hibernation of this engine, so replacing it would strand
        // the workflows the replaced one hibernated.
        if (hibernation.isEnabled()) {
            throw new IllegalStateException("Hibernation is enabled already.");
        }
        if (started) {
            throw new IllegalStateException("Hibernation can only be enabled before the engine starts.");
        }
        this.hibernation = new WorkflowEngineHibernationSupport(
                scheduler, clock, liveWorkflows, runningWorkflows, restorer, starter, idleDelay, wakeTimeout,
                wakeExecutor
        );
    }

    /**
     * Returns whether this engine may hibernate workflows, as its workflow module enabled hibernation for it, see
     * {@link #enableHibernation(WorkflowScheduler, Clock, Duration, Duration, Executor)}. Without hibernation, every
     * live workflow of this engine is in memory.
     *
     * @return {@code true} when hibernation is enabled
     */
    @Internal
    public boolean isHibernationEnabled() {
        return hibernation.isEnabled();
    }

    /**
     * Returns the hibernation of this engine: the {@link WorkflowEngineHibernationSupport} once its workflow module
     * enabled hibernation, see {@link #enableHibernation(WorkflowScheduler, Clock, Duration, Duration, Executor)}, and
     * {@link NoHibernation} before, or without it. The {@link WorkflowCancellationService} of the module reads it for
     * each request that finds its workflow hibernated, to wake that workflow.
     *
     * @return the hibernation of this engine
     */
    @Internal
    public Hibernation hibernation() {
        return hibernation;
    }

    /**
     * Returns the identifiers of the workflows this engine hibernated. They are live, but not in memory.
     * <p>
     * Visits every workflow live in this engine.
     *
     * @return an unmodifiable snapshot of the identifiers of the hibernated workflows, or an empty set when hibernation
     * is not enabled
     */
    @Internal
    public Set<String> hibernatedWorkflowIds() {
        return hibernation.hibernatedWorkflowIds();
    }

    @Override
    public MessageStream.Empty<Message> handle(EventMessage event,
                                               ProcessingContext context) {
        TrackingToken currentToken = TrackingToken.fromContext(context).orElse(null);
        logger.trace("Handling event [{}] with id [{}] and token [{}].",
                     event.type(), event.identifier(), currentToken);
        checkpointingSupport.getAndSetTriggerFrom(context);

        var segment = Segment.fromContext(context).orElse(null);
        if (segmentClaims.hasFailed(segment)) {
            // Fails the batch, so nothing of it is checkpointed, and makes the processor release the segment.
            return MessageStream.failed(new IllegalStateException(
                    "The workflow executions of segment " + segment.getSegmentId() + " could not be restored."
            ));
        }
        try {
            // Before any early return: the running-workflows index follows every lifecycle event of an owned instance,
            // whether or not this engine runs that instance.
            runningWorkflows.apply(segment, event);
            route(segment, event, context);
            segmentClaims.activateOnDelivery(segment, currentToken, activation);
            checkpointingSupport.requestCheckpoint(segment, currentToken);
        } catch (RuntimeException | Error e) {
            if (segment != null) {
                holdCheckpointAndRelease(segment, event);
            }
            throw e;
        }
        logger.trace("Event [{}] handled successfully.", event.identifier());
        return MessageStream.empty();
    }

    /**
     * Keeps a segment that failed to handle the given event from storing a position past it, and asks the processor to
     * release the segment, so its next claim handles the event again.
     * <p>
     * Until the claim ends, no checkpoint and no release of it may store a position past the event. The release is
     * asked for as well, as after a failed restore, because an error handler that does not propagate the failure keeps
     * the segment claimed: it would then handle later events, never the failed one, and never store a position again.
     */
    private void holdCheckpointAndRelease(Segment segment, EventMessage event) {
        segmentClaims.holdCheckpoint(segment);
        logger.warn("Handling event [{}] failed in segment {}; asking the processor to release the segment, so its "
                            + "next claim handles the event again.",
                    event.identifier(), segment.getSegmentId());
        try {
            segmentReleaser.accept(segment);
        } catch (RuntimeException e) {
            logger.error("Could not ask the processor to release segment {}.", segment.getSegmentId(), e);
        }
    }

    /**
     * Routes the given event to the workflows of the given segment: an event of a workflow to that workflow, any other
     * event to the workflows it starts and to the workflows waiting for it.
     */
    private void route(@Nullable Segment segment, EventMessage event, ProcessingContext context) {
        // An event published by a workflow through the publish primitive carries the publisher's workflowId but is a
        // business event for everyone else, so it never takes the owner-only path below.
        boolean workflowPublishedEvent = MetadataUtils.isPublishStep(event.metadata());
        if (!workflowPublishedEvent && MetadataUtils.hasWorkflowId().test(event.metadata())) {
            var workflowId = MetadataUtils.getWorkflowId(event.metadata());
            // Instance partitioning: a segment only processes instances it owns.
            if (!WorkflowSegmentOwnership.ownedBy(segment, workflowId)) {
                logger.debug("Ignoring event [{}] for workflowId [{}] - instance is owned by another segment than {}.",
                             event.type(), workflowId, segment);
                return;
            }
            // Skip events whose workflowId isn't owned by this engine (expected in multi-module setups). A hibernated
            // workflow wakes for an event of its own, through this engine's hibernation, under the lock of the
            // workflow.
            if (!liveWorkflows.deliver(workflowId, event, context, wakeForEvent)) {
                logger.debug("Ignoring event [{}] for workflowId [{}] - no matching execution in this engine.",
                             event.type(), workflowId);
                return;
            }
        } else {
            // handle starting of new processes. Events published by a workflow through the publish primitive take
            // this branch as well: they may start or wake other instances, and the publishing instance is one of the
            // owned executions below, so delivering it there records the published step and unblocks the publisher.
            checkAndCreateNewInstance(event, context);
            // route external events to workflows waiting for them. Business events without a unique start candidate
            // are broadcast to every segment (sequenced by SequencingPolicy.BROADCAST); the ownership filter keeps
            // the wake exactly-once per instance - only the owning segment's delivery reaches an execution.
            // The registry lists this segment's instances only: it is node-wide, and every segment handles a
            // broadcast event, so visiting all of them once per claimed segment would cost quadratic work.
            // With hibernation, hibernated workflows waiting for the event are woken as well, through this engine's
            // hibernation, under the lock of each workflow.
            liveWorkflows.broadcast(segment, event, context, wakeForEvent);
        }
    }

    private void checkAndCreateNewInstance(EventMessage eventMessage,
                                           ProcessingContext processingContext) {
        // For brand-new starts, only the highest-registered version starts instances.
        // Older registered versions stay available for replay routing (selected later in the execution path
        // based on state.workflowDefinitionId().version(), itself sourced from the workflow's started event metadata).
        // Same-version duplicates start in parallel only if their workflowIdProviders produce distinct ids;
        // otherwise the second start is rejected as a same-version duplicate in resolveWorkflowIdForNewInstance.
        // The "multiple definitions at the same version" warning is emitted ONCE at engine startup (see
        // checkForSameVersionDuplicates) rather than per event.
        var configurations = workflowConfigurationRegistry.getHighestVersionConfigurations(eventMessage.type());
        // Start placement: each segment only starts instances it owns. Unique start candidates
        // arrive at the owning segment directly; broadcast business events arrive everywhere and the ownership
        // guard keeps the start exactly-once.
        var segment = Segment.fromContext(processingContext).orElse(null);

        configurations.forEach(configuration -> {
            if (!configuration.predicate().test(eventMessage, processingContext)) {
                return;
            }

            var workflowConfiguration = configuration.configuration();
            var baseWorkflowId = workflowConfiguration.workflowIdProvider().apply(eventMessage);
            // Must precede the ownership guard: that guard derives a segment key from the id, so an id the provider
            // could not derive would fail the work package there.
            if (!hasDerivedWorkflowId(baseWorkflowId, workflowConfiguration, eventMessage)) {
                return;
            }
            if (!WorkflowSegmentOwnership.ownedBy(segment, baseWorkflowId)) {
                logger.debug("Not starting workflow '{}': the instance is owned by another segment than {}.",
                             baseWorkflowId, segment);
                return;
            }
            // Follows the ownership guard, so only the segment owning an invalid id logs it, once per event.
            if (!hasValidWorkflowId(baseWorkflowId, workflowConfiguration, eventMessage)) {
                return;
            }
            // Under the lock of the workflow, so it cannot hibernate between the check for a duplicate and the save.
            try (var ignored = liveWorkflows.lock(baseWorkflowId)) {
                if (!liveWorkflows.isHibernated(baseWorkflowId)) {
                    startNewInstance(baseWorkflowId, workflowConfiguration, eventMessage, processingContext, segment);
                    return;
                }
            }
            logger.warn("A workflow with id '{}' is already running, hibernated; ignoring the new start request "
                                + "triggered by event '{}'. Workflow ids MUST be unique, so the start is treated as a "
                                + "duplicate.",
                        baseWorkflowId, eventMessage.type().qualifiedName());
        });
    }

    private void startNewInstance(String baseWorkflowId,
                                  WorkflowConfiguration<?> workflowConfiguration,
                                  EventMessage eventMessage,
                                  ProcessingContext processingContext,
                                  @Nullable Segment segment) {
        var workflowId = resolveWorkflowIdForNewInstance(
                liveWorkflows.find(baseWorkflowId), baseWorkflowId, workflowConfiguration.workflowVersion(),
                eventMessage
        );

        if (workflowId == null) {
            return;
        }

        var payload = requireNonNull(eventMessage.payloadAs(PAYLOAD_TYPE), "Error converting initial payload");
        processingContext.putResource(WorkflowConfigurationRegistry.RESOURCE_KEY, workflowConfigurationRegistry);
        var workflowContext = workflowConfiguration.workflowContextFactory().createContext(
                payload, workflowId, processingContext, workflowConfiguration
        );

        // Exclusive with the activation of the segment, which starts every execution it finds that does not run
        // yet: either that activation finds this execution and starts it, or this start does, never both.
        segmentClaims.runAlways(segment, bodyStart -> {
            var claim = segment == null ? null : segmentClaims.claimOf(segment).orElse(null);
            var execution = liveWorkflows.enter(workflowId, hibernation.mayHibernate(claim), () -> {
                logger.debug("Creating a new workflow '{}' with payload '{}'", workflowId, eventMessage.payload());
                return workflowConfiguration.workflowExecutionFactory().create(workflowContext);
            });
            // Takes cancellation requests as it enters, also while its claim catches up and its body does not start.
            liveWorkflows.markCancellable(execution);
            hibernation.allow(execution, claim);
            if (bodyStart == BodyStart.NOW) {
                starter.start(execution);
            }
        });
    }

    @Override
    public boolean hasUnsafeCheckpointWork(Segment segment) {
        return unsafeExecutionsOf(segment).anyMatch(WorkflowExecution::isRunning);
    }

    /**
     * {@inheritDoc}
     * <p>
     * A segment whose claim failed to restore its executions holds its checkpoint as well: it handled nothing it can
     * vouch for, so neither a checkpoint request nor its release may advance its stored position. So does a segment
     * whose claim failed to handle an event, until that claim ends, so the next claim handles that event again.
     */
    @Override
    public boolean holdsCheckpoint(Segment segment) {
        return segmentClaims.holdsCheckpoint(segment)
                || unsafeExecutionsOf(segment).anyMatch(execution -> !execution.isRunning());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Decides to write the changes to the index of running workflows pending now. A flush within the processor's
     * context also writes the hibernation hints recorded while the checkpoint waits for its workflows, see
     * {@link RunningWorkflowsProjection#decideCheckpoint(Segment)}. Fails right away when the claim of the segment did
     * not finish restoring, and the flush fails when the changes cannot be written in time; either fails the
     * checkpoint.
     */
    @Override
    public WorkflowEngineCheckpointingSupport.CheckpointFlush decideCheckpointFlush(Segment segment) {
        return runningWorkflows.decideCheckpoint(segment);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Writes the pending changes to the index of running workflows within a bounded time. Fails when they cannot be
     * written in time, which releases the segment without advancing its stored token.
     */
    @Override
    public CompletableFuture<Void> flushRelease(Segment segment) {
        return runningWorkflows.flushForRelease(segment);
    }

    /**
     * Adds a checkpoint latch across the current set of {@link WorkflowExecution WorkflowExecutions} owned by the given
     * {@code segment} that are still performing tasks.
     * <p>
     * The given {@code latch} is attached to all {@code WorkflowExecutions} that still have tasks to perform. Or in
     * other terms, executions that are "unsafe" to checkpoint on. When no execution is unsafe at the moment the latch
     * is attached, the latch runs straight away so safety is re-checked, covering a workflow that appended work
     * concurrently.
     *
     * @param segment the segment whose checkpoint is being advanced
     * @param latch   the latch to invoke after all unsafe {@link WorkflowExecution WorkflowExecutions} have reached it
     */
    @Override
    public void addCheckpointLatch(Segment segment, Runnable latch) {
        Set<WorkflowExecution> executions = new HashSet<>();
        for (var entry : liveWorkflows.safePoints().unsafeOf(segment)) {
            var execution = entry.execution();
            if (execution.hasUnsafeCheckpointWork()) {
                if (execution.isRunning()) {
                    executions.add(execution);
                }
            } else {
                liveWorkflows.safePoints().markSafe(entry);
            }
        }

        if (executions.isEmpty()) {
            latch.run();
            return;
        }

        for (WorkflowExecution execution : executions) {
            execution.addCheckpointLatch(latch);
        }
    }

    /**
     * Retrieve all workflow executions.
     *
     * @return set of currently running workflow executions.
     */
    public Set<WorkflowExecution> workflowExecutions() {
        return liveWorkflows.listInMemory();
    }

    /**
     * Records the stream position that restored segments must reach before their workflow bodies start.
     *
     * @param latestToken latest token observed while starting the processor
     * @return a completed future after startup validation
     */
    public synchronized CompletableFuture<Void> start(@Nullable TrackingToken latestToken) {
        started = true;
        segmentClaims.initialize(latestToken);
        hibernation.start();
        workflowConfigurationRegistry.warnAboutSameVersionDuplicates();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Restores and starts the workflow executions owned by a segment this node just claimed.
     * <p>
     * Restoration is per segment rather than per node: a node materializes only the instances of the segments it holds,
     * so a segment migrating between nodes carries its instances with it. When another node dies, the coordinator hands
     * its segments to a surviving node and this callback rebuilds their instances there, without restarting anything.
     * <p>
     * The instances are materialized from their current durable state and started immediately. With hibernation, the
     * instances whose hibernation hint the claim accepted stay out of memory instead, as hibernated instances of the
     * claim.
     *
     * @param segment          the segment that was claimed
     * @param claimedFrom      the stored position the segment resumes from, or {@code null} when it has consumed
     *                         nothing. This is what tells a segment sitting at the end of the stream from one far
     *                         behind, on a node that never held the segment as well as on one that did
     * @param sourcingContext  short-lived context used to load durable workflow state
     * @param executionContext independent context used as the parent of the restored workflow executions. Reusing
     *                         {@code sourcingContext} here is invalid because restored workflow bodies run
     *                         asynchronously and can outlive the claim callback. If such a body appends an event after
     *                         the sourcing unit of work has entered {@code COMMIT}, Axon can no longer register the
     *                         required {@code PREPARE_COMMIT} handler, and the workflow cannot persist its resumed,
     *                         timed-out, or terminal state
     * @return a future that completes once the segment's executions are restored and, unless the segment is still
     * catching up, started. It completes exceptionally when loading durable state fails or does not finish within
     * {@link #DEFAULT_RESTORE_TIMEOUT}. The claim then starts none of the executions it restored, and stays inert until
     * the processor released the segment
     */
    public CompletableFuture<Void> restoreWorkflowsFor(Segment segment,
                                                       @Nullable TrackingToken claimedFrom,
                                                       ProcessingContext sourcingContext,
                                                       ProcessingContext executionContext) {
        // Gives the restored executions the segment that owns them, which an execution started by an event has from
        // the context of that event, so a workflow handler resolving the segment, such as through a Segment
        // parameter, gets it either way. Correct because every execution restored on this context is owned by it.
        executionContext.putResource(Segment.RESOURCE_KEY, segment);
        // Drops what a previous claim of this segment left behind when it ended without a release, including its
        // catch-up state and executions: every claim catches up on its own, and restores from durable state.
        // The executions are stopped before the hibernations are dropped: an execution of the previous claim that
        // hibernates meanwhile does so under that claim, which ended, so it stays in memory and is stopped here.
        var claim = segmentClaims.startClaim(segment, executionContext);
        stopExecutionsOf(segment);
        hibernation.dropOverlapping(segment);
        runningWorkflows.discard(segment);
        return claimLoader
                // Half the restore timeout for streaming the index tail, so a stalled stream still leaves time to
                // fall back to the full running-workflow list and restore the instances within the restore timeout.
                .load(segment, claimedFrom, sourcingContext, restoreTimeout.dividedBy(2))
                // Without hibernation, a hibernated instance is restored like any other, and its hint cleared.
                .thenApply(claimed -> hibernation.withAcceptedHints(claimed))
                .thenCompose(claimed -> restoreRunningWorkflows(claim,
                                                                claimed.runningWorkflowIds(),
                                                                sourcingContext,
                                                                executionContext)
                        .thenApply(claimed::withoutStale))
                .orTimeout(restoreTimeout.toMillis(), TimeUnit.MILLISECONDS)
                // Only a claim that loaded in time tracks the segment: a late load must not replace the state of
                // events handled meanwhile. A segment that is not tracked writes nothing, and its checkpoints fail.
                // Tracked before the hibernated instances are adopted, so it records their wakes.
                .thenAccept(claimed -> {
                    runningWorkflows.initialize(segment, claimed);
                    hibernation.adopt(claim, claimed.hibernated());
                })
                .whenComplete((restored, failure) -> {
                    if (failure != null) {
                        // Before the release: instances still restoring are dropped, and every event of the segment
                        // fails until the processor released it. The instances it did restore never start, and are
                        // dropped as well, also when the segment is never released here.
                        claim.fail(() -> stopExecutionsOf(segment));
                        // Outside the lock of the claim, as dropping takes the locks of the workflows.
                        hibernation.dropOf(claim);
                        onFailedRestore(segment, failure);
                    } else {
                        segmentClaims.activateOrAwaitCatchUp(claim, claimedFrom, activation);
                    }
                });
    }

    /**
     * Drops the workflow executions owned by a segment this node no longer holds.
     * <p>
     * In-flight steps are stopped rather than cancelled, exactly as on {@link #shutdown()}: no cancellation events are
     * emitted, so the node claiming the segment next resumes each instance from its persisted state.
     * <p>
     * The index of running workflows of the segment stops tracking it, dropping changes that were not written: the
     * processor wrote them through {@link #flushRelease(Segment)} right before, and when that failed, it did not
     * advance the stored token, so the next claim streams the events of the dropped changes again.
     *
     * @param segment the segment that was released
     * @return a future completed after this engine has requested every released execution to stop
     */
    public CompletableFuture<Void> releaseWorkflowsFor(Segment segment) {
        // Also for a segment without executions: a later claim of it has to catch up again. First, so an instance its
        // claim still restores is either dropped below or not added at all.
        segmentClaims.release(segment);
        stopExecutionsOf(segment);
        hibernation.dropOverlapping(segment);
        runningWorkflows.discard(segment);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Stops and drops the workflow executions owned by the given {@code segment}, without emitting any event.
     */
    private void stopExecutionsOf(Segment segment) {
        var stopped = liveWorkflows.listInMemoryOwnedBy(segment);
        if (!stopped.isEmpty()) {
            logger.info("Stopping {} workflow execution(s) of segment {}.", stopped.size(), segment.getSegmentId());
            stopped.forEach(WorkflowExecution::stopForShutdown);
            // A stopped execution skips the termination handler, so its cleanup happens here.
            stopped.forEach(liveWorkflows::leave);
        }
    }

    /**
     * Returns whether the given restore {@code failure} is an {@link UnrecognizedWorkflowDefinitionException},
     * possibly wrapped in a {@link java.util.concurrent.CompletionException} by the future chain it surfaced from.
     */
    private static boolean isUnrecognizedDefinition(Throwable failure) {
        return failure instanceof UnrecognizedWorkflowDefinitionException
                || failure.getCause() instanceof UnrecognizedWorkflowDefinitionException;
    }

    /**
     * Asks the processor to release a segment whose restore failed. The processor itself only logs a failing claim
     * listener and keeps the segment, which would process it without its running instances.
     */
    private void onFailedRestore(Segment segment, Throwable failure) {
        logger.error("Restoring the workflow executions of segment {} failed or did not complete within {}; asking "
                             + "the processor to release the segment so a later claim restores it again.",
                     segment.getSegmentId(), restoreTimeout, failure);
        try {
            segmentReleaser.accept(segment);
        } catch (RuntimeException e) {
            logger.error("Could not ask the processor to release segment {}.", segment.getSegmentId(), e);
        }
    }

    /**
     * Restores the given running instances of the claimed segment, each on its own, so one unrestorable instance
     * cannot abort the restore of the others.
     *
     * @return a future completing with the ids of the instances that turned out not to run, as their sourced state is
     * terminal
     */
    private CompletableFuture<Set<String>> restoreRunningWorkflows(SegmentClaim claim,
                                                                   Set<String> ownedIds,
                                                                   ProcessingContext sourcingContext,
                                                                   ProcessingContext executionContext) {
        var segment = claim.segment();
        if (ownedIds.isEmpty()) {
            logger.debug("No running workflows to rehydrate for segment {}.", segment.getSegmentId());
            return CompletableFuture.completedFuture(Set.of());
        }
        logger.debug("Rehydrating {} running workflow execution(s) of segment {}.",
                     ownedIds.size(), segment.getSegmentId());
        Set<String> stale = ConcurrentHashMap.newKeySet();
        var rehydrations = ownedIds.stream()
                                   .map(workflowId -> restoreWorkflow(claim,
                                                                      workflowId,
                                                                      sourcingContext,
                                                                      executionContext)
                                           .thenAccept(running -> {
                                               if (!running) {
                                                   stale.add(workflowId);
                                               }
                                           })
                                           // Per instance, so one unrestorable workflow cannot abort the whole
                                           // restore pass. Aborting it would fail the segment claim, leaving every
                                           // other instance of that segment unrehydrated until the segment is claimed
                                           // again. The skipped instance keeps its durable state and is restored by a
                                           // later claim.
                                           .exceptionally(failure -> {
                                               if (isUnrecognizedDefinition(failure)) {
                                                   logger.trace("Skipping workflow '{}' of segment {}: its definition "
                                                                        + "belongs to another WorkflowModule, not "
                                                                        + "this engine.",
                                                                workflowId, segment.getSegmentId());
                                               } else {
                                                   logger.error("Skipping workflow '{}' of segment {}: it could not "
                                                                        + "be restored. It stays durable and is not "
                                                                        + "running on this node.",
                                                                workflowId, segment.getSegmentId(), failure);
                                               }
                                               return null;
                                           }))
                                   .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(rehydrations).thenApply(restored -> Set.copyOf(stale));
    }

    /**
     * Restores one instance of the claimed segment through the {@link WorkflowRestorer restorer}, which sources it in a
     * unit of work of its own, so the position the restored execution resumes appending from is its own, and brings
     * the restored execution into memory under the given claim, unless that claim is no longer current.
     *
     * @param claim            the claim restoring the instance
     * @param workflowId       id of the instance to restore
     * @param claimContext     the context of the claim, whose resources the instance is sourced with
     * @param executionContext the context the restored execution runs on
     * @return a future completing with {@code false} when the sourced state of the instance is terminal, so there is
     * nothing to restore, and with {@code true} otherwise: once the instance entered memory, or when the claim failed
     * or ended before it could
     */
    private CompletableFuture<Boolean> restoreWorkflow(SegmentClaim claim,
                                                       String workflowId,
                                                       ProcessingContext claimContext,
                                                       ProcessingContext executionContext) {
        return restorer.restore(workflowId, claimContext, executionContext)
                       .thenApply(restored -> {
                           // Guarded, so the workflow cannot hibernate, wake or be adopted while it enters memory.
                           restored.ifPresent(execution -> hibernation.restoreGuarded(
                                   workflowId, () -> enterRestored(claim, workflowId, execution)
                           ));
                           return restored.isPresent();
                       });
    }

    /**
     * Brings the given restored execution into memory under the given claim, unless that claim is no longer current:
     * in place of a hibernation of the workflow under another claim, if any, see
     * {@link Hibernation#restoreOverHibernation(SegmentClaim, String, WorkflowExecution, boolean)}. Runs guarded
     * against the hibernations, wakes and adoptions of the workflow, see
     * {@link Hibernation#restoreGuarded(String, java.util.function.Supplier)}.
     *
     * @return {@code true} when the execution entered memory, {@code false} when the claim failed or ended meanwhile
     */
    private boolean enterRestored(SegmentClaim claim, String workflowId, WorkflowExecution execution) {
        // Only while the claim is current: an instance that finishes restoring after its claim failed or the segment
        // was released would otherwise stay behind, and run from the state it sourced back then. A restore that
        // outlives its claim also leaves a hibernation of the workflow under another claim alone.
        boolean added = claim.runIfCurrent(bodyStart -> {
            boolean mayHibernate = hibernation.mayHibernate(claim);
            // Takes the place of a hibernation of the workflow under another claim, if any; else the restore enters.
            boolean restoredOver = hibernation.restoreOverHibernation(claim, workflowId, execution, mayHibernate);
            if (!restoredOver && liveWorkflows.enter(workflowId, mayHibernate, () -> execution) != execution) {
                // A claim drops what earlier claims of its segment left behind, so only a concurrent restore of the
                // instance gets here. Never adopt its execution: moving the append position of that execution to this
                // read would let it run from its own, older state.
                throw new IllegalStateException(
                        "Workflow '%s' already has an execution on this node.".formatted(workflowId)
                );
            }
            hibernation.allow(execution, claim);
            return true;
        });
        if (!added) {
            logger.debug("Not restoring workflow '{}': the claim of segment {} failed or ended meanwhile.",
                         workflowId, claim.segment().getSegmentId());
        }
        return added;
    }

    /**
     * Removes terminal workflow executions and starts the remaining restored executions of the given segment: the
     * {@link SegmentClaims.Activation activation} of a claim that caught up with the startup head, which this engine
     * hands to every call of its {@link SegmentClaims} that may activate a claim.
     * <p>
     * Both passes are scoped to the segment they run for. Instances of other segments are none of this segment's
     * business: they belong to a segment at its own position, possibly still replaying, and starting one here would run
     * its body on behalf of a segment that never asked for it.
     *
     * @param segment the segment whose executions are considered, or {@code null} outside a segmented processor
     * @param phase   startup phase in which the executions are started
     */
    private void removeTerminalAndStartRestoredWorkflowExecutions(@Nullable Segment segment, String phase) {
        if (segment != null) {
            // The hibernated workflows the claim adopted may now wake for their deadline or a cancellation.
            hibernation.arm(segment);
        }
        var terminalExecutions = liveWorkflows.listInMemoryOwnedBy(segment)
                                              .stream()
                                              .filter(execution -> execution.state().workflowStatus().isTerminal())
                                              .toList();
        terminalExecutions.forEach(liveWorkflows::leave);
        var executionsToStart = liveWorkflows.listInMemoryOwnedBy(segment)
                                             .stream()
                                             .filter(execution -> !execution.isRunning())
                                             .toList();
        if (executionsToStart.isEmpty()) {
            logger.debug("No restored workflow executions require startup {}.", phase);
            return;
        }
        logger.debug("Starting {} restored workflow execution(s) {}.", executionsToStart.size(), phase);
        executionsToStart.forEach(starter::start);
        logger.debug("Started {} restored workflow execution(s) {}.", executionsToStart.size(), phase);
    }

    /**
     * Shuts downs the engine and removes all running workflow executions.
     * <p>
     * Before clearing the repository, all in-flight step futures are interrupted so that workflow driver threads parked
     * in {@code sleepAsync} / {@code waitForEvent} / async {@code execute} can exit. This is an interrupt, not a
     * cancellation: no {@code <Step>Cancelled} / {@code <Workflow>Cancelled} events are emitted, so the persisted event
     * stream still reflects the most recent {@code <Step>Started} and the step resumes on the next app start. Without
     * this, a graceful shutdown can hang because the workflow executor (e.g. a virtual-thread-per-task executor) blocks
     * on {@code close()} waiting for those threads to terminate. See issue #125.
     * <p>
     * A workflow driver that exits after this call skips its termination handler, so it touches no engine state.
     */
    public void shutdown() {
        segmentClaims.stop();
        hibernation.stop();
        var executions = liveWorkflows.listInMemory();
        logger.info("Shutting down WorkflowEngine: interrupting running steps of {} workflow instance(s).",
                    executions.size());
        for (var execution : executions) {
            execution.stopForShutdown();
        }
        liveWorkflows.clear();
    }

    /**
     * Returns the executions in memory owned by the given segment that have work in progress that makes a checkpoint
     * of the segment unsafe. A segment token only covers the instances that segment owns, so executions of other
     * segments must not hold it back.
     */
    private Stream<WorkflowExecution> unsafeExecutionsOf(Segment segment) {
        return liveWorkflows.safePoints().unsafeOf(segment)
                            .stream()
                            .map(LiveWorkflows.InMemory::execution)
                            .filter(WorkflowExecution::hasUnsafeCheckpointWork);
    }

    /**
     * Returns the sequencing policy derived from this engine's workflow definitions, which delivers each event to the
     * segment owning the workflow instance it affects.
     *
     * @return the sequencing policy of this engine.
     */
    public SequencingPolicy<EventMessage> segmentedRouting() {
        return segmentedRouting;
    }
}
