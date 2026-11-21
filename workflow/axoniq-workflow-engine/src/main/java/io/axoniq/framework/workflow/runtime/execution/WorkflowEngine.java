/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.runtime.util.ProcessingContextUtils;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStore;
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

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
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
     * a segment's durable state before it fails the claim.
     * <p>
     * The workflow store is remote, so the load can stall without failing. The processor logs a failing claim listener
     * and carries on, so an unbounded load would leave the segment claimed here with none of its instances running. A
     * release needs no such bound: the processor already caps it at its claim extension threshold.
     */
    static final Duration DEFAULT_RESTORE_TIMEOUT = Duration.ofSeconds(30);
    /**
     * Time between two reads of the position of a segment that waits to catch up with the startup head.
     */
    static final Duration CATCH_UP_POLL_INTERVAL = Duration.ofMillis(200);
    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngine.class);
    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private final WorkflowExecutionRepository workflowExecutionRepository;
    private final WorkflowCancellationService workflowCancellationService;
    private final WorkflowStore workflowStore;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final WorkflowEngineCatchUpSupport catchUpSupport = new WorkflowEngineCatchUpSupport();
    private final WorkflowEngineSequencingPolicy segmentedRouting;
    private final UnsafeCheckpointWorkIndex checkpointWorkIndex = new UnsafeCheckpointWorkIndex();
    // Package-private so a test can shrink it instead of waiting out the production timeout.
    Duration restoreTimeout = DEFAULT_RESTORE_TIMEOUT;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;
    private volatile Function<Segment, Optional<TrackingToken>> segmentPositions = segment -> Optional.empty();
    private volatile Consumer<Segment> segmentReleaser = segment -> {
    };
    private volatile boolean shutDown;

    /**
     * Creates a new workflow engine. Be sure to follow-up construction of a {@code WorkflowEngine} with an invocation
     * of {@link #setCheckpointingSupport(WorkflowEngineCheckpointingSupport)}, as otherwise checkpointing support is
     * unavailable.
     *
     * @param workflowConfigurationRegistry registry containing a
     *                                      {@link
     *                                      io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration
     *                                      workflow configuration}
     * @param workflowExecutionRepository   repository dedicated towards {@link WorkflowExecution} storage and
     *                                      retrieval
     * @param workflowCancellationService   workflow cancellation service
     * @param workflowStore                 dedicated store for workflow information, like {@link RunningWorkflows} and
     *                                      {@link WorkflowState}
     * @param unitOfWorkFactory             a unit of work factory dedicated to construct a unit of work during
     *                                      {@link #start(TrackingToken, boolean)} of this engine
     */
    @Internal
    public WorkflowEngine(
            WorkflowConfigurationRegistry<?> workflowConfigurationRegistry,
            WorkflowExecutionRepository workflowExecutionRepository,
            WorkflowCancellationService workflowCancellationService,
            WorkflowStore workflowStore,
            UnitOfWorkFactory unitOfWorkFactory
    ) {
        this.workflowConfigurationRegistry = requireNonNull(
                workflowConfigurationRegistry, "The WorkflowConfigurationRegistry must not be null."
        );
        this.workflowExecutionRepository = requireNonNull(
                workflowExecutionRepository, "The WorkflowExecutionRepository must not be null."
        );
        this.workflowCancellationService = requireNonNull(
                workflowCancellationService, "The WorkflowCancellationService must not be null."
        );
        this.workflowStore = requireNonNull(workflowStore, "The WorkflowStore must not be null.");
        this.unitOfWorkFactory = requireNonNull(unitOfWorkFactory, "The UnitOfWorkFactory must not be null.");
        this.segmentedRouting = new WorkflowEngineSequencingPolicy(workflowConfigurationRegistry);
        EntitlementManager.INSTANCE.registerAddon(WorkflowAxoniqAddon.class);
    }

    /**
     * Returns the position the given context's sourcing ended at.
     * <p>
     * The current Event Store does not add its append position to the processing context when sourcing. Retrieve it
     * from the context-bound transaction until {@link ConsistencyMarker} provides a corresponding context accessor. The
     * missing-component path keeps focused engine tests that construct an engine without a full workflow configuration
     * independent of the Event Store.
     *
     * @param sourcingContext context used to source the workflow state
     * @return the marker at which sourcing ended
     */
    @Nullable
    private static ConsistencyMarker sourcedAt(ProcessingContext sourcingContext) {
        try {
            return sourcingContext.component(EventStore.class).transaction(sourcingContext).appendPosition();
        } catch (ComponentNotFoundException e) {
            return null;
        }
    }

    /**
     * Sets the checkpointing support required by this engine.
     * <p>
     * The component is set outside the
     * {@link #WorkflowEngine(WorkflowConfigurationRegistry, WorkflowExecutionRepository, WorkflowCancellationService,
     * WorkflowStore, UnitOfWorkFactory)}, because they require <b>this</b> {@code WorkflowEngine} itself to function.
     * Hence, a cyclic dependency would exist upon start-up if completion otherwise.
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
        this.segmentPositions = requireNonNull(segmentPositions, "The segment positions must not be null.");
    }

    /**
     * Sets how this engine asks the processor to release a segment that failed to handle an event.
     * <p>
     * An error handler that does not propagate the failure of an event keeps the segment claimed, while the engine
     * stores no position past that event until the segment is released and claimed again. Without a releaser, such a
     * segment stays claimed, and handles the failed event again only once it is claimed again.
     *
     * @param segmentReleaser asks the processor to release the given segment
     */
    @Internal
    public void setSegmentReleaser(Consumer<Segment> segmentReleaser) {
        this.segmentReleaser = requireNonNull(segmentReleaser, "The segment releaser must not be null.");
    }

    @Override
    public MessageStream.Empty<Message> handle(EventMessage event,
                                               ProcessingContext context) {
        TrackingToken currentToken = TrackingToken.fromContext(context).orElse(null);
        logger.trace("Handling event [{}] with id [{}] and token [{}].",
                     event.type(), event.identifier(), currentToken);
        checkpointingSupport.getAndSetTriggerFrom(context);

        var segment = Segment.fromContext(context).orElse(null);
        try {
            route(segment, event, context);
            activateSegmentWhenCaughtUp(segment, currentToken);
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
     * Until the segment is claimed again, no checkpoint and no release may store a position past the event. The release
     * is asked for as well, because an error handler that does not propagate the failure keeps the segment claimed: it
     * would then handle later events, never the failed one, and never store a position again.
     */
    private void holdCheckpointAndRelease(Segment segment, EventMessage event) {
        catchUpSupport.holdCheckpoint(segment);
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
            // Skip events whose workflowId isn't owned by this engine (expected in multi-module setups).
            var executionOpt = workflowExecutionRepository.findById(workflowId);
            if (executionOpt.isEmpty()) {
                logger.debug("Ignoring event [{}] for workflowId [{}] - no matching execution in this engine.",
                             event.type(), workflowId);
                return;
            }
            executionOpt.get().onEvent(event, context);
        } else {
            // handle starting of new processes. Events published by a workflow through the publish primitive take
            // this branch as well: they may start or wake other instances, and the publishing instance is one of the
            // owned executions below, so delivering it there records the published step and unblocks the publisher.
            checkAndCreateNewInstance(event, context);
            // route external events to workflows waiting for them. Business events without a unique start candidate
            // are broadcast to every segment (sequenced by SequencingPolicy.BROADCAST); the ownership filter keeps
            // the wake exactly-once per instance - only the owning segment's delivery reaches an execution.
            // The filter is pushed into the repository so the returned set holds this segment's instances only:
            // the repository is node-wide, so materializing all of them once per claimed segment is wasted work.
            for (var execution : workflowExecutionRepository.findAll(ownedBy(segment))) {
                execution.onEvent(event, context);
            }
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
            var workflowId = resolveWorkflowIdForNewInstance(
                    workflowExecutionRepository, baseWorkflowId, workflowConfiguration.workflowVersion(), eventMessage
            );

            if (workflowId == null) {
                return;
            }

            var payload = requireNonNull(eventMessage.payloadAs(PAYLOAD_TYPE), "Error converting initial payload");
            processingContext.putResource(WorkflowConfigurationRegistry.RESOURCE_KEY, workflowConfigurationRegistry);
            var workflowContext = workflowConfiguration.workflowContextFactory().createContext(
                    payload, workflowId, processingContext, workflowConfiguration
            );

            var execution = workflowExecutionRepository.save(workflowId, () -> {
                logger.debug("Creating a new workflow '{}' with payload '{}'", workflowId, eventMessage.payload());
                return workflowConfiguration.workflowExecutionFactory().create(workflowContext);
            });
            registerCancellation(execution);
            checkpointWorkIndex.register(execution.workflowId(), execution::registerCheckpointWorkStateListener);
            if (isSegmentCaughtUp(segment)) {
                execute(execution);
            }
        });
    }

    @Override
    public boolean hasUnsafeCheckpointWork(Segment segment) {
        return unsafeExecutionsOf(segment).anyMatch(WorkflowExecution::isRunning);
    }

    @Override
    public boolean holdsCheckpoint(Segment segment) {
        return catchUpSupport.holdsCheckpoint(segment)
                || unsafeExecutionsOf(segment).anyMatch(execution -> !execution.isRunning());
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
        unsafeWorkflowIdsOf(segment).forEach(
                workflowId -> workflowExecutionRepository.findById(workflowId).ifPresentOrElse(
                        execution -> {
                            if (execution.hasUnsafeCheckpointWork()) {
                                if (execution.isRunning()) {
                                    executions.add(execution);
                                }
                            } else {
                                checkpointWorkIndex.markSafe(workflowId);
                            }
                        },
                        () -> checkpointWorkIndex.markSafe(workflowId))
        );

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
        return workflowExecutionRepository.findAll();
    }

    /**
     * Records the stream position that restored segments must reach before their workflow bodies start.
     *
     * @param latestToken latest token observed while starting the processor
     * @return a completed future after startup validation
     */
    public CompletableFuture<Void> start(@Nullable TrackingToken latestToken) {
        catchUpSupport.initialize(latestToken);
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
     * The instances are materialized from their current durable state and started immediately.
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
     * replaying, started. It completes exceptionally when loading durable state does not finish within
     * {@link #DEFAULT_RESTORE_TIMEOUT}, failing the claim rather than holding the segment
     */
    public CompletableFuture<Void> restoreWorkflowsFor(Segment segment,
                                                       @Nullable TrackingToken claimedFrom,
                                                       ProcessingContext sourcingContext,
                                                       ProcessingContext executionContext) {
        // Gives the restored executions the segment that owns them, which an execution started by an event has from
        // the context of that event, so a workflow handler resolving the segment, such as through a Segment
        // parameter, gets it either way. Correct because every execution restored on this context is owned by it.
        executionContext.putResource(Segment.RESOURCE_KEY, segment);
        // A new claim handles the events a failed event held the checkpoint of the previous claim for again.
        catchUpSupport.startClaim(segment);
        return loadRunningWorkflows(segment, claimedFrom, sourcingContext, executionContext)
                .orTimeout(restoreTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((restored, failure) -> logFailedRestore(segment, failure))
                .thenRun(() -> startRestoredWorkflowsWhenCaughtUp(segment, claimedFrom));
    }

    /**
     * Drops the workflow executions owned by a segment this node no longer holds.
     * <p>
     * In-flight steps are stopped rather than cancelled, exactly as on {@link #shutdown()}: no cancellation events are
     * emitted, so the node claiming the segment next resumes each instance from its persisted state.
     *
     * @param segment the segment that was released
     * @return a future completed after this engine has requested every released execution to stop
     */
    public CompletableFuture<Void> releaseWorkflowsFor(Segment segment) {
        // Also for a segment without executions: a later claim of it has to catch up again.
        catchUpSupport.release(segment);
        var released = workflowExecutionRepository.findAll(ownedBy(segment));
        if (released.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        logger.info("Releasing segment {}: stopping {} workflow execution(s).",
                    segment.getSegmentId(), released.size());
        released.forEach(WorkflowExecution::stopForShutdown);
        // An execution stopped for recovery skips the termination handler, so the release does its cleanup.
        released.forEach(execution -> removeExecution(execution.workflowId()));
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Returns whether the given restore {@code failure} is an {@link UnrecognizedWorkflowDefinitionException},
     * possibly wrapped in a {@link java.util.concurrent.CompletionException} by the future chain it surfaced from.
     */
    private static boolean isUnrecognizedDefinition(Throwable failure) {
        return failure instanceof UnrecognizedWorkflowDefinitionException
                || failure.getCause() instanceof UnrecognizedWorkflowDefinitionException;
    }

    private void logFailedRestore(Segment segment, @Nullable Throwable failure) {
        if (failure != null) {
            logger.error("Restoring the workflow executions of segment {} did not complete within {}; failing the "
                                 + "claim so the coordinator can hand the segment out again.",
                         segment.getSegmentId(), restoreTimeout, failure);
        }
    }

    private void startRestoredWorkflowsWhenCaughtUp(Segment segment, @Nullable TrackingToken claimedFrom) {
        if (!catchUpSupport.startsAfterClaim(segment, claimedFrom)) {
            logger.info("Segment {} is catching up; restored workflow executions start after catch-up.",
                        segment.getSegmentId());
            awaitCatchUp(segment, catchUpSupport.startWaiting(segment));
            return;
        }
        removeTerminalAndStartRestoredWorkflowExecutions(segment, "on claim of segment " + segment.getSegmentId());
    }

    /**
     * Polls the position of the given {@code segment} until it covers the startup head.
     * <p>
     * A segment normally learns it caught up from an event it handles at or after the startup head. But the processor
     * skips events of other segments without handing them over, so a segment whose backlog holds no event of its own
     * would never learn it, and its restored workflows would never start. Its position, which the processor advances
     * past skipped events as well, tells it instead. Polling ends once the segment caught up, either way, or its claim
     * ended.
     */
    private void awaitCatchUp(Segment segment, Object wait) {
        CompletableFuture.delayedExecutor(CATCH_UP_POLL_INTERVAL.toMillis(), TimeUnit.MILLISECONDS).execute(() -> {
            if (shutDown || !catchUpSupport.isWaiting(segment, wait)) {
                return;
            }
            try {
                segmentPositions.apply(segment)
                                .ifPresent(position -> activateSegmentWhenCaughtUp(segment, position));
            } catch (RuntimeException e) {
                logger.warn("Could not read the position of segment {} while it catches up.",
                            segment.getSegmentId(), e);
            }
            awaitCatchUp(segment, wait);
        });
    }

    private void activateSegmentWhenCaughtUp(@Nullable Segment segment, @Nullable TrackingToken currentToken) {
        if (!catchUpSupport.recordDelivery(segment, currentToken)) {
            return;
        }
        removeTerminalAndStartRestoredWorkflowExecutions(segment, "after catch-up");
    }

    private boolean isSegmentCaughtUp(@Nullable Segment segment) {
        return catchUpSupport.isCaughtUp(segment);
    }

    private Predicate<WorkflowExecution> ownedBy(@Nullable Segment segment) {
        return execution -> WorkflowSegmentOwnership.ownedBy(segment, execution.workflowId());
    }

    private CompletableFuture<Void> loadRunningWorkflows(Segment segment,
                                                         @Nullable TrackingToken claimedFrom,
                                                         ProcessingContext sourcingContext,
                                                         ProcessingContext executionContext) {
        return workflowStore.loadRunningWorkflows(sourcingContext)
                            .thenCompose(runningWorkflows -> {
                                var ownedIds = runningWorkflows.workflowIds()
                                                               .stream()
                                                               .filter(id -> WorkflowSegmentOwnership.ownedBy(
                                                                       segment, id
                                                               ))
                                                               .toList();
                                if (ownedIds.isEmpty()) {
                                    logger.debug("No running workflows to rehydrate for segment {}.",
                                                 segment.getSegmentId());
                                    return CompletableFuture.completedFuture(null);
                                }
                                logger.debug("Rehydrating {} running workflow execution(s) of segment {}.",
                                             ownedIds.size(), segment.getSegmentId());
                                var rehydrations = ownedIds.stream()
                                                           .map(workflowId -> restoreWorkflow(workflowId,
                                                                                              sourcingContext,
                                                                                              executionContext)
                                                                   // Per instance, so one unrestorable workflow cannot
                                                                   // abort the whole restore pass. Aborting it would
                                                                   // fail the segment claim callback, which the
                                                                   // processor only logs, leaving every other instance
                                                                   // of that segment permanently unrehydrated while the
                                                                   // node looks healthy. The skipped instance keeps its
                                                                   // durable state and is restored by a later claim.
                                                                   .exceptionally(failure -> {
                                                                       if (isUnrecognizedDefinition(failure)) {
                                                                           logger.trace(
                                                                                   "Skipping workflow '{}' of "
                                                                                           + "segment {}: its "
                                                                                           + "definition belongs to "
                                                                                           + "another WorkflowModule, "
                                                                                           + "not this engine.",
                                                                                   workflowId,
                                                                                   segment.getSegmentId());
                                                                       } else {
                                                                           logger.error(
                                                                                   "Skipping workflow '{}' of segment {}: "
                                                                                           + "it could not be restored. It "
                                                                                           + "stays durable and is not "
                                                                                           + "running on this node.",
                                                                                   workflowId,
                                                                                   segment.getSegmentId(),
                                                                                   failure);
                                                                       }
                                                                       return null;
                                                                   }))
                                                           .toArray(CompletableFuture[]::new);
                                return CompletableFuture.allOf(rehydrations);
                            });
    }

    /**
     * Sources one instance in a unit of work of its own and restores it at the position that read ended at.
     * <p>
     * The position has to be the instance's own: a transaction shared by every instance of the claim ends at the lowest
     * of its reads, and a previous owner appending while the claim is still reading leaves that position before an
     * event the claim itself sourced. The restored execution would then be rejected by its own history and stop, even
     * though its state is current. Reading one instance per transaction leaves each position at or after that
     * instance's last event, so only a write that lands after this read rejects the execution, which is a foreign
     * writer and exactly what should stop it.
     *
     * @param workflowId       id of the instance to restore
     * @param executionContext the context the restored execution runs on
     * @return a future completing once the instance is restored
     */
    private CompletableFuture<Void> restoreWorkflow(String workflowId,
                                                    ProcessingContext claimContext,
                                                    ProcessingContext executionContext) {
        return unitOfWorkFactory
                .create("restore-" + workflowId)
                .executeWithResult(sourcingContext -> {
                    // Everything the claim reads with, except its event store transaction: that is what makes this
                    // read, and the position it ends at, this instance's own.
                    ProcessingContextUtils.copyResources(claimContext, sourcingContext);
                    return workflowStore
                            .loadWorkflow(workflowId, sourcingContext)
                            .thenAccept(state -> restoreWorkflow(workflowId,
                                                                 state,
                                                                 executionContext,
                                                                 sourcedAt(sourcingContext)));
                })
                .thenApply(ignored -> null);
    }

    private void restoreWorkflow(String workflowId,
                                 WorkflowState state,
                                 ProcessingContext executionContext,
                                 @Nullable ConsistencyMarker restoredAt) {
        var definitionId = state.workflowDefinitionId();
        var workflowName = definitionId.qualifiedName().toString();
        // Same routing as the replay path: a body may have moved its recorded version forward with
        // ctx.migrateVersion(...) to a version no definition is statically registered under, so an exact lookup alone
        // cannot restore it.
        var workflowConfiguration = workflowConfigurationRegistry
                .getWorkflowConfiguration(definitionId)
                .or(() -> workflowConfigurationRegistry.findClosestRegisteredVersion(
                        workflowName, definitionId.version()
                ))
                .or(() -> workflowConfigurationRegistry.findClosestHigherRegisteredVersion(
                        workflowName, definitionId.version()
                ))
                .orElseThrow(() -> new UnrecognizedWorkflowDefinitionException(
                        "No workflow configuration found for workflow '%s' with definition %s."
                                .formatted(workflowId, definitionId)
                ));
        executionContext.putResource(WorkflowConfigurationRegistry.RESOURCE_KEY, workflowConfigurationRegistry);
        var workflowContext = workflowConfiguration.workflowContextFactory().createContext(
                state.payload(),
                workflowId,
                executionContext,
                workflowConfiguration
        );
        var execution = workflowConfiguration.workflowExecutionFactory().create(workflowContext);
        execution.initializeState(state);
        var storedExecution = workflowExecutionRepository.save(workflowId, () -> execution);
        storedExecution.restoreAppendPosition(restoredAt);
        checkpointWorkIndex.register(
                storedExecution.workflowId(), storedExecution::registerCheckpointWorkStateListener
        );
    }

    /**
     * Removes terminal workflow executions and starts the remaining restored executions of the given segment.
     * <p>
     * Both passes are scoped to the segment they run for. Instances of other segments are none of this segment's
     * business: they belong to a segment at its own position, possibly still replaying, and starting one here would run
     * its body on behalf of a segment that never asked for it.
     *
     * @param segment the segment whose executions are considered, or {@code null} outside a segmented processor
     * @param phase   startup phase in which the executions are started
     */
    private void removeTerminalAndStartRestoredWorkflowExecutions(@Nullable Segment segment, String phase) {
        var owned = ownedBy(segment);
        var terminal = owned.and(execution -> execution.state().workflowStatus().isTerminal());
        var terminalExecutions = workflowExecutionRepository.findAll(terminal);
        workflowExecutionRepository.removeAll(terminal);
        terminalExecutions.forEach(execution -> checkpointWorkIndex.markSafe(execution.workflowId()));
        var executionsToStart = workflowExecutionRepository.findAll(owned.and(execution -> !execution.isRunning()));
        if (executionsToStart.isEmpty()) {
            logger.debug("No restored workflow executions require startup {}.", phase);
            return;
        }
        logger.debug("Starting {} restored workflow execution(s) {}.", executionsToStart.size(), phase);
        for (var execution : executionsToStart) {
            registerCancellation(execution);
            execute(execution);
        }
        logger.debug("Started {} restored workflow execution(s) {}.", executionsToStart.size(), phase);
    }

    private void execute(WorkflowExecution execution) {
        execution.workflowExecutionOperations()
                 .processingContext()
                 .whenComplete(context -> {
                     try {
                         logger.debug("Executing workflow execution with id: {}", execution.workflowId());
                         execution.execute(
                                 finished -> {
                                     logger.debug("Workflow {} finished with status {}, removing it from repository",
                                                  execution.workflowId(),
                                                  finished.state().workflowStatus());
                                     removeExecution(execution.workflowId());
                                 }
                         ).whenComplete((ignored, failure) -> {
                             if (failure != null) {
                                 logger.error("Workflow {} stopped with an unhandled execution failure",
                                              execution.workflowId(), failure);
                             }
                         });
                     } catch (Throwable t) {
                         throw new RuntimeException("Error during workflow execution", t);
                     }
                 });
    }

    private void registerCancellation(WorkflowExecution execution) {
        if (execution instanceof WorkflowCancellationProvider provider) {
            workflowCancellationService.register(execution.workflowId(), provider.workflowCancellation());
        } else {
            throw new IllegalStateException(
                    "Provided workflow execution {} does not implement WorkflowCancellationProvider, but the cancellation was registered.".formatted(
                            execution.workflowId()));
        }
    }

    /**
     * Removes an execution that leaves this engine and releases its safe point.
     */
    private void removeExecution(String workflowId) {
        workflowExecutionRepository.remove(workflowId);
        workflowCancellationService.unregister(workflowId);
        checkpointWorkIndex.markSafe(workflowId);
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
        shutDown = true;
        var executions = workflowExecutionRepository.findAll();
        logger.info("Shutting down WorkflowEngine: interrupting running steps of {} workflow instance(s).",
                    executions.size());
        for (var execution : executions) {
            execution.stopForShutdown();
        }
        workflowExecutionRepository.clear();
        checkpointWorkIndex.clear();
        workflowCancellationService.clear();
    }

    /**
     * Narrows the unsafe-execution index to the instances owned by the given segment. A segment token only covers the
     * instances that segment owns, so executions of other segments must not hold it back, and their index entries must
     * not be marked safe on its behalf either.
     */
    private Stream<String> unsafeWorkflowIdsOf(Segment segment) {
        return checkpointWorkIndex.unsafeWorkflowIds()
                                  .stream()
                                  .filter(workflowId -> WorkflowSegmentOwnership.ownedBy(segment, workflowId));
    }

    private Stream<WorkflowExecution> unsafeExecutionsOf(Segment segment) {
        return unsafeWorkflowIdsOf(segment).map(workflowExecutionRepository::findById)
                                           .flatMap(Optional::stream)
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
