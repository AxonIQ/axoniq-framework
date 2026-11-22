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
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.runtime.util.ProcessingContextUtils;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.Registration;
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

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
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
    /**
     * Time between two reads of the position of a segment that waits to catch up with the startup head.
     */
    static final Duration CATCH_UP_POLL_INTERVAL = Duration.ofMillis(200);
    /**
     * Number of consecutive polls without a known position after which a segment waiting to catch up is no longer
     * polled: the processor no longer holds it, so its position will not become known again.
     */
    static final int CATCH_UP_POLLS_WITHOUT_POSITION = 10;
    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngine.class);
    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private final WorkflowExecutionRepository workflowExecutionRepository;
    private final WorkflowCancellationService workflowCancellationService;
    private final WorkflowStore workflowStore;
    private final UnitOfWorkFactory unitOfWorkFactory;
    // Package-private so a test can start a new claim of a segment without stopping the executions of the current one.
    final WorkflowEngineCatchUpSupport catchUpSupport = new WorkflowEngineCatchUpSupport();
    private final WorkflowEngineSequencingPolicy segmentedRouting;
    private final RunningWorkflowsProjection runningWorkflows;
    private final RunningWorkflowsClaimLoader claimLoader;
    private final UnsafeCheckpointWorkIndex checkpointWorkIndex = new UnsafeCheckpointWorkIndex();
    // The cancellation each execution registered, so only that registration is removed when the execution leaves.
    private final Map<WorkflowExecution, WorkflowCancellation> registeredCancellations = new ConcurrentHashMap<>();
    // Package-private so a test can shrink it instead of waiting out the production timeout.
    Duration restoreTimeout = DEFAULT_RESTORE_TIMEOUT;
    // Runs each catch-up poll once the poll interval passed. Package-private so a test can run the polls itself,
    // instead of waiting for the interval to pass.
    Consumer<Runnable> catchUpPolls =
            poll -> CompletableFuture.delayedExecutor(CATCH_UP_POLL_INTERVAL.toMillis(), TimeUnit.MILLISECONDS)
                                     .execute(poll);
    private WorkflowEngineCheckpointingSupport checkpointingSupport;
    private volatile Function<Segment, Optional<TrackingToken>> segmentPositions = segment -> Optional.empty();
    private volatile Consumer<Segment> segmentReleaser = segment -> {
    };
    private volatile boolean shutDown;
    // Package-private so a test can tell whether hibernation is enabled.
    @Nullable
    WorkflowEngineHibernationSupport hibernation;
    private Duration wakeTimeout = DEFAULT_RESTORE_TIMEOUT;

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
     * @param workflowExecutionRepository   repository dedicated towards {@link WorkflowExecution} storage and
     *                                      retrieval
     * @param workflowCancellationService   workflow cancellation service
     * @param workflowStore                 dedicated store for workflow information, like {@link RunningWorkflows} and
     *                                      {@link WorkflowState}
     * @param workflowIndex                 the index persisting the running workflows per segment
     * @param indexName                     the name scoping this engine's entries in the {@code workflowIndex},
     *                                      unique per engine sharing the index
     * @param unitOfWorkFactory             a unit of work factory dedicated to construct a unit of work during
     *                                      {@link #start(TrackingToken)} of this engine
     */
    @Internal
    public WorkflowEngine(
            WorkflowConfigurationRegistry<?> workflowConfigurationRegistry,
            WorkflowExecutionRepository workflowExecutionRepository,
            WorkflowCancellationService workflowCancellationService,
            WorkflowStore workflowStore,
            WorkflowIndex workflowIndex,
            String indexName,
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
        this.runningWorkflows = new RunningWorkflowsProjection(workflowIndex, indexName);
        this.claimLoader = new RunningWorkflowsClaimLoader(workflowIndex, indexName, workflowStore);
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
     * WorkflowStore, WorkflowIndex, String, UnitOfWorkFactory)}, because they require <b>this</b>
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
     * Hibernation is a decision of this node only: it writes nothing, and a claim of a segment restores every running
     * workflow of that segment, hibernated here or not.
     *
     * @param scheduler    schedules the passes and the deadlines of hibernated workflows
     * @param clock        the clock the scheduler measures deadlines and idle time with
     * @param idleDelay    the time a workflow must make no progress before it hibernates
     * @param wakeTimeout  the time a wake may take to load a workflow from the event store
     * @param wakeExecutor runs the wakes for a deadline or a cancellation
     */
    @Internal
    public void enableHibernation(WorkflowScheduler scheduler,
                                  Clock clock,
                                  Duration idleDelay,
                                  Duration wakeTimeout,
                                  Executor wakeExecutor) {
        this.wakeTimeout = requireNonNull(wakeTimeout, "The wake timeout must not be null.");
        this.hibernation = new WorkflowEngineHibernationSupport(
                scheduler, clock, workflowExecutionRepository, workflowCancellationService, checkpointWorkIndex,
                catchUpSupport, this::wakeWorkflow, idleDelay, wakeExecutor
        );
    }

    /**
     * Returns the identifiers of the workflows this engine hibernated. They are live, but not in memory.
     * <p>
     * The set is an unmodifiable view that follows the workflows that hibernate and wake; copy it for a snapshot.
     *
     * @return a view of the identifiers of the hibernated workflows, empty when hibernation is not enabled
     */
    @Internal
    public Set<String> hibernatedWorkflowIds() {
        var support = hibernation;
        return support == null ? Set.of() : Collections.unmodifiableSet(support.hibernatedWorkflowIds());
    }

    /**
     * Registers a listener that every wake of a workflow this engine hibernated tells the identifier of that workflow,
     * once the workflow is back in memory and before it leaves the {@link #hibernatedWorkflowIds() hibernated ones}.
     * <p>
     * A lookup that checks the hibernated workflows both before and after it lists the executions in memory, as the
     * {@link SimpleWorkflowManager} does, registers one for its duration, so it also learns of a workflow that
     * hibernates after the first check and wakes before the second.
     *
     * @param listener the listener to tell the identifier of each workflow that wakes, on the thread that wakes it
     * @return the registration that removes the listener again, which does nothing when hibernation is not enabled
     */
    @Internal
    public Registration onWake(Consumer<String> listener) {
        requireNonNull(listener, "The wake listener must not be null.");
        var support = hibernation;
        return support == null ? () -> true : support.onWake(listener);
    }

    @Override
    public MessageStream.Empty<Message> handle(EventMessage event,
                                               ProcessingContext context) {
        TrackingToken currentToken = TrackingToken.fromContext(context).orElse(null);
        logger.trace("Handling event [{}] with id [{}] and token [{}].",
                     event.type(), event.identifier(), currentToken);
        checkpointingSupport.getAndSetTriggerFrom(context);

        var segment = Segment.fromContext(context).orElse(null);
        if (catchUpSupport.hasFailed(segment)) {
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
     * Until the claim ends, no checkpoint and no release of it may store a position past the event. The release is
     * asked for as well, as after a failed restore, because an error handler that does not propagate the failure keeps
     * the segment claimed: it would then handle later events, never the failed one, and never store a position again.
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
            if (!deliver(workflowId, event, context)) {
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
            // The repository lists this segment's instances only: it is node-wide, and every segment handles a
            // broadcast event, so visiting all of them once per claimed segment would cost quadratic work.
            // With hibernation, hibernated workflows waiting for the event are woken as well.
            var support = hibernation;
            if (support != null) {
                support.broadcast(segment, event, context);
            } else {
                for (var execution : workflowExecutionRepository.findAllOwnedBy(segment)) {
                    execution.onEvent(event, context);
                }
            }
        }
    }

    /**
     * Delivers an event of the given workflow to its execution, or, with hibernation, wakes the workflow when it is
     * hibernated.
     *
     * @return {@code false} when this engine runs no such workflow, which is expected in multi-module setups
     */
    private boolean deliver(String workflowId, EventMessage event, ProcessingContext context) {
        var support = hibernation;
        if (support != null) {
            return support.deliver(workflowId, event, context);
        }
        var execution = workflowExecutionRepository.findById(workflowId);
        execution.ifPresent(found -> found.onEvent(event, context));
        return execution.isPresent();
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
            var support = hibernation;
            if (support == null) {
                startNewInstance(baseWorkflowId, workflowConfiguration, eventMessage, processingContext, segment);
                return;
            }
            // Under the lock of the workflow, so it cannot hibernate between the check for a duplicate and the save.
            boolean started = support.unlessHibernated(baseWorkflowId, () -> startNewInstance(
                    baseWorkflowId, workflowConfiguration, eventMessage, processingContext, segment
            ));
            if (!started) {
                logger.warn("A workflow with id '{}' is already running, hibernated; ignoring the new start request "
                                    + "triggered by event '{}'. Workflow ids MUST be unique, so the start is treated "
                                    + "as a duplicate.",
                            baseWorkflowId, eventMessage.type().qualifiedName());
            }
        });
    }

    private void startNewInstance(String baseWorkflowId,
                                  WorkflowConfiguration<?> workflowConfiguration,
                                  EventMessage eventMessage,
                                  ProcessingContext processingContext,
                                  @Nullable Segment segment) {
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

        // Exclusive with the activation of the segment, which starts every execution it finds that does not run
        // yet: either that activation finds this execution and starts it, or this start does, never both.
        catchUpSupport.runExclusively(segment, caughtUp -> {
            var execution = workflowExecutionRepository.save(workflowId, () -> {
                logger.debug("Creating a new workflow '{}' with payload '{}'", workflowId, eventMessage.payload());
                return workflowConfiguration.workflowExecutionFactory().create(workflowContext);
            });
            registerCancellation(execution);
            registerCheckpointWork(execution, segment == null ? null : catchUpSupport.claimOf(segment).orElse(null));
            if (caughtUp) {
                execute(execution);
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
        return catchUpSupport.holdsCheckpoint(segment)
                || unsafeExecutionsOf(segment).anyMatch(execution -> !execution.isRunning());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Writes the pending changes to the index of running workflows. Fails when they cannot be written in time, or when
     * the claim of the segment did not finish restoring, which fails the checkpoint.
     */
    @Override
    public CompletableFuture<Void> flushCheckpoint(Segment segment, @Nullable ProcessingContext context) {
        return runningWorkflows.flushForCheckpoint(segment, context);
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
        var support = hibernation;
        if (support != null) {
            support.start();
        }
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
        var claim = catchUpSupport.startClaim(segment, executionContext);
        stopExecutionsOf(segment);
        dropHibernatedWorkflowsOf(segment);
        runningWorkflows.discard(segment);
        return claimLoader
                // Half the restore timeout for streaming the index tail, so a stalled stream still leaves time to
                // fall back to the full running-workflow list and restore the instances within the restore timeout.
                .load(segment, claimedFrom, sourcingContext, restoreTimeout.dividedBy(2))
                .thenCompose(claimed -> restoreRunningWorkflows(claim,
                                                                claimed.runningWorkflowIds(),
                                                                sourcingContext,
                                                                executionContext)
                        .thenApply(claimed::withoutStale))
                .orTimeout(restoreTimeout.toMillis(), TimeUnit.MILLISECONDS)
                // Only a claim that loaded in time tracks the segment: a late load must not replace the state of
                // events handled meanwhile. A segment that is not tracked writes nothing, and its checkpoints fail.
                .thenAccept(claimed -> runningWorkflows.initialize(segment, claimed))
                .whenComplete((restored, failure) -> {
                    if (failure != null) {
                        // Before the release: instances still restoring are dropped, and every event of the segment
                        // fails until the processor released it. The instances it did restore never start, and are
                        // dropped as well, also when the segment is never released here.
                        catchUpSupport.fail(claim, () -> stopExecutionsOf(segment));
                        onFailedRestore(segment, failure);
                    } else {
                        startRestoredWorkflowsWhenCaughtUp(claim, claimedFrom);
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
        catchUpSupport.release(segment);
        stopExecutionsOf(segment);
        dropHibernatedWorkflowsOf(segment);
        runningWorkflows.discard(segment);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Forgets the hibernated workflows of the given segment, and of every segment overlapping it: the claim of their
     * segment, here or on another node, restores them from the event store.
     */
    private void dropHibernatedWorkflowsOf(Segment segment) {
        var support = hibernation;
        if (support != null) {
            support.drop(segment);
        }
    }

    /**
     * Stops and drops the workflow executions owned by the given {@code segment}, without emitting any event.
     */
    private void stopExecutionsOf(Segment segment) {
        var stopped = workflowExecutionRepository.findAllOwnedBy(segment);
        if (!stopped.isEmpty()) {
            logger.info("Stopping {} workflow execution(s) of segment {}.", stopped.size(), segment.getSegmentId());
            stopped.forEach(WorkflowExecution::stopForShutdown);
            // A stopped execution skips the termination handler, so its cleanup happens here.
            stopped.forEach(this::removeExecution);
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

    private void startRestoredWorkflowsWhenCaughtUp(WorkflowEngineCatchUpSupport.Claim claim,
                                                    @Nullable TrackingToken claimedFrom) {
        var segment = claim.segment();
        boolean activated = catchUpSupport.activate(
                claim,
                claimedFrom,
                () -> removeTerminalAndStartRestoredWorkflowExecutions(
                        segment, "on claim of segment " + segment.getSegmentId()
                )
        );
        if (!activated) {
            logger.info("Segment {} is catching up; restored workflow executions start after catch-up.",
                        segment.getSegmentId());
            awaitCatchUp(claim, 0);
        }
    }

    /**
     * Polls the position of the given {@code segment} until it covers the startup head.
     * <p>
     * A segment normally learns it caught up from an event it handles at or after the startup head. But the processor
     * skips events of other segments without handing them over, so a segment whose backlog holds no event of its own
     * would never learn it, and its restored workflows would never start. Its position, which the processor advances
     * past skipped events as well, tells it instead. Polling ends once the claim caught up, either way, or ended; a
     * poller of an ended claim can no longer activate anything. It also ends once the position of the segment was
     * unknown for {@link #CATCH_UP_POLLS_WITHOUT_POSITION} polls in a row: the processor no longer holds the segment,
     * for example because it was claimed elsewhere. A position that is briefly unknown, right after the claim, does
     * not end it. An event the segment handles still activates the claim after polling ended.
     *
     * @param claim                the claim waiting to catch up
     * @param pollsWithoutPosition the number of consecutive polls so far that found no position of the segment
     */
    private void awaitCatchUp(WorkflowEngineCatchUpSupport.Claim claim, int pollsWithoutPosition) {
        catchUpPolls.accept(() -> {
            if (shutDown || !catchUpSupport.isWaiting(claim)) {
                return;
            }
            var segment = claim.segment();
            int withoutPosition = pollsWithoutPosition;
            try {
                var position = segmentPositions.apply(segment);
                if (position.isPresent()) {
                    withoutPosition = 0;
                    catchUpSupport.activate(
                            claim,
                            position.get(),
                            () -> removeTerminalAndStartRestoredWorkflowExecutions(segment, "after catch-up")
                    );
                } else {
                    withoutPosition++;
                }
            } catch (RuntimeException e) {
                logger.warn("Could not read the position of segment {} while it catches up.",
                            segment.getSegmentId(), e);
            }
            if (withoutPosition >= CATCH_UP_POLLS_WITHOUT_POSITION) {
                logger.info("Segment {} is no longer listed by the processor; no longer polling its position to "
                                    + "catch up.",
                            segment.getSegmentId());
                return;
            }
            awaitCatchUp(claim, withoutPosition);
        });
    }

    private void activateSegmentWhenCaughtUp(@Nullable Segment segment, @Nullable TrackingToken currentToken) {
        catchUpSupport.activateOnDelivery(
                segment, currentToken, () -> removeTerminalAndStartRestoredWorkflowExecutions(segment, "after catch-up")
        );
    }

    /**
     * Restores the given running instances of the claimed segment, each on its own, so one unrestorable instance
     * cannot abort the restore of the others.
     *
     * @return a future completing with the ids of the instances that turned out not to run, as their sourced state is
     * terminal
     */
    private CompletableFuture<Set<String>> restoreRunningWorkflows(WorkflowEngineCatchUpSupport.Claim claim,
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
     * Sources one instance in a unit of work of its own and restores it at the position that read ended at.
     * <p>
     * The position has to be the instance's own: a transaction shared by every instance of the claim ends at the lowest
     * of its reads, and a previous owner appending while the claim is still reading leaves that position before an
     * event the claim itself sourced. The restored execution would then be rejected by its own history and stop, even
     * though its state is current. Reading one instance per transaction leaves each position at or after that
     * instance's last event, so only a write that lands after this read rejects the execution, which is a foreign
     * writer and exactly what should stop it.
     *
     * @param claim            the claim restoring the instance
     * @param workflowId       id of the instance to restore
     * @param executionContext the context the restored execution runs on
     * @return a future completing with {@code true} once the instance is restored, or with {@code false} when its
     * sourced state is terminal, so there is nothing to restore
     */
    private CompletableFuture<Boolean> restoreWorkflow(WorkflowEngineCatchUpSupport.Claim claim,
                                                       String workflowId,
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
                            .thenApply(state -> {
                                var status = state.workflowStatus();
                                if (status.isTerminal()) {
                                    logger.debug("Not restoring workflow '{}': its state is {}.", workflowId, status);
                                    return false;
                                }
                                restoreWorkflow(claim, workflowId, state, executionContext,
                                                sourcedAt(sourcingContext));
                                return true;
                            });
                });
    }

    /**
     * Restores the given state of a workflow into an execution of the given claim, unless that claim is no longer
     * current.
     *
     * @return the restored execution, or empty when the claim failed or ended meanwhile
     */
    private Optional<WorkflowExecution> restoreWorkflow(WorkflowEngineCatchUpSupport.Claim claim,
                                                        String workflowId,
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
        // Only while the claim is current: an instance that finishes restoring after its claim failed or the segment
        // was released would otherwise stay behind, and run from the state it sourced back then.
        boolean added = catchUpSupport.runWhileCurrent(claim, () -> {
            var storedExecution = workflowExecutionRepository.save(workflowId, () -> execution);
            if (storedExecution != execution) {
                // A claim drops what earlier claims of its segment left behind, so only a concurrent restore of the
                // instance gets here. Never adopt its execution: moving the append position of that execution to this
                // read would let it run from its own, older state.
                throw new IllegalStateException(
                        "Workflow '%s' already has an execution on this node.".formatted(workflowId)
                );
            }
            execution.restoreAppendPosition(restoredAt);
            registerCheckpointWork(execution, claim);
        });
        if (!added) {
            logger.debug("Not restoring workflow '{}': the claim of segment {} failed or ended meanwhile.",
                         workflowId, claim.segment().getSegmentId());
            return Optional.empty();
        }
        return Optional.of(execution);
    }

    /**
     * Loads a hibernated workflow back from the event store under the claim it hibernated under, and starts it once
     * that claim caught up, as the hibernation support's {@link WorkflowEngineHibernationSupport.WorkflowLoader}.
     * <p>
     * The workflow is sourced in a unit of work of its own, like a restore on a claim, so the position its read ended
     * at is its own. That unit of work holds the resources of the execution context of the claim, except its event
     * store transaction, the way a restore on a claim holds those of the claim's context. The execution runs on the
     * execution context of the claim. The given action runs on the loaded execution before its body starts. When that
     * action or the start fails, the loaded execution leaves memory again, so the workflow stays only hibernated.
     *
     * @return {@code false} when the workflow ended elsewhere, or the claim ended meanwhile
     */
    private boolean wakeWorkflow(WorkflowEngineCatchUpSupport.Claim claim,
                                 String workflowId,
                                 Consumer<WorkflowExecution> beforeStart) {
        var segment = claim.segment();
        var executionContext = requireNonNull(claim.executionContext(), "A claim without execution context.");
        var sourced = unitOfWorkFactory
                .create("wake-" + workflowId)
                .executeWithResult(sourcingContext -> {
                    ProcessingContextUtils.copyResources(executionContext, sourcingContext);
                    return workflowStore.loadWorkflow(workflowId, sourcingContext)
                                        .thenApply(state -> new SourcedWorkflow(state, sourcedAt(sourcingContext)));
                })
                .orTimeout(wakeTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .join();
        if (sourced.state().workflowStatus().isTerminal()) {
            return false;
        }
        var restored = restoreWorkflow(claim, workflowId, sourced.state(), executionContext, sourced.position());
        if (restored.isEmpty()) {
            return false;
        }
        var execution = restored.get();
        try {
            beforeStart.accept(execution);
            // Started here only once the claim caught up; before that, its activation starts it with the restored
            // ones.
            catchUpSupport.runWhileCurrent(claim, caughtUp -> {
                if (caughtUp && !execution.isRunning()) {
                    registerCancellation(execution);
                    execute(execution);
                }
            });
        } catch (RuntimeException e) {
            // Partly woken: this execution leaves memory again, and the workflow stays hibernated. The hibernation
            // support registers the cancellation stand-in of the hibernation again, should this execution have
            // replaced it, and fails the requests that waited for this execution.
            execution.stopForShutdown();
            unregisterCancellation(execution);
            if (workflowExecutionRepository.remove(workflowId, execution)) {
                checkpointWorkIndex.markSafe(workflowId);
            }
            leftMemory(execution, e);
            throw e;
        }
        return true;
    }

    /**
     * The state of a workflow sourced on its own, and the position that read ended at.
     */
    private record SourcedWorkflow(WorkflowState state, @Nullable ConsistencyMarker position) {

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
        var terminalExecutions = workflowExecutionRepository.findAllOwnedBy(segment)
                                                           .stream()
                                                           .filter(execution -> execution.state()
                                                                                         .workflowStatus()
                                                                                         .isTerminal())
                                                           .toList();
        terminalExecutions.forEach(execution -> {
            if (workflowExecutionRepository.remove(execution.workflowId(), execution)) {
                unregisterCancellation(execution);
                checkpointWorkIndex.markSafe(execution.workflowId());
                leftMemory(execution, null);
            }
        });
        var executionsToStart = workflowExecutionRepository.findAllOwnedBy(segment)
                                                           .stream()
                                                           .filter(execution -> !execution.isRunning())
                                                           .toList();
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
                                     removeExecution(execution);
                                 }
                         ).whenComplete((ignored, failure) -> {
                             if (failure != null) {
                                 logger.error("Workflow {} stopped with an unhandled execution failure",
                                              execution.workflowId(), failure);
                             }
                             // An execution that hibernated left memory without leaving the engine: its cancellation
                             // is no longer registered, but still remembered, which would keep it in memory.
                             if (workflowExecutionRepository.findById(execution.workflowId()).orElse(null)
                                     != execution) {
                                 unregisterCancellation(execution);
                             }
                         });
                     } catch (Throwable t) {
                         throw new RuntimeException("Error during workflow execution", t);
                     }
                 });
    }

    /**
     * Registers the cancellation of the given execution for its workflow, replacing the registration of the workflow,
     * and remembers it, so the execution unregisters exactly this registration when it leaves.
     * <p>
     * Package-private so a test can register the cancellation of an execution that left memory.
     */
    void registerCancellation(WorkflowExecution execution) {
        if (execution instanceof WorkflowCancellationProvider provider) {
            var cancellation = provider.workflowCancellation();
            var support = hibernation;
            var registered = support == null ? cancellation : support.decorate(execution, cancellation);
            registeredCancellations.put(execution, registered);
            workflowCancellationService.register(execution.workflowId(), registered);
        } else {
            throw new IllegalStateException(
                    "Provided workflow execution {} does not implement WorkflowCancellationProvider, but the cancellation was registered.".formatted(
                            execution.workflowId()));
        }
    }

    /**
     * Lets the checkpoint work index follow the given execution, and lets the execution hibernate under the given
     * claim when hibernation is enabled.
     *
     * @param claim the claim the execution was started or restored under, or {@code null} outside a segmented
     *              processor, in which case it never hibernates
     */
    private void registerCheckpointWork(WorkflowExecution execution,
                                        WorkflowEngineCatchUpSupport.@Nullable Claim claim) {
        var support = hibernation;
        if (support == null) {
            checkpointWorkIndex.register(execution.workflowId(), execution::registerCheckpointWorkStateListener);
            return;
        }
        checkpointWorkIndex.register(
                execution.workflowId(),
                listener -> execution.registerCheckpointWorkStateListener(support.decorate(execution, listener))
        );
        support.allow(execution, claim);
    }

    /**
     * Unregisters the cancellation the given execution registered, unless another registration replaced it.
     */
    private void unregisterCancellation(WorkflowExecution execution) {
        var registered = registeredCancellations.remove(execution);
        if (registered != null) {
            workflowCancellationService.unregister(execution.workflowId(), registered);
        }
    }

    /**
     * Tells the hibernation support, when enabled, that the given execution left memory, so the requests that waited
     * for it to run them are answered.
     *
     * @param failure the failure that made the execution leave, or {@code null} when it left otherwise
     */
    private void leftMemory(WorkflowExecution execution, @Nullable Throwable failure) {
        var support = hibernation;
        if (support != null) {
            support.leftMemory(execution, failure);
        }
    }

    /**
     * Removes the given execution when it leaves this engine, and releases its safe point and its cancellation. Leaves
     * the registrations of another execution of the workflow, or of its hibernation, that replaced this one in place.
     */
    private void removeExecution(WorkflowExecution execution) {
        var workflowId = execution.workflowId();
        unregisterCancellation(execution);
        if (workflowExecutionRepository.remove(workflowId, execution)) {
            checkpointWorkIndex.markSafe(workflowId);
        }
        leftMemory(execution, null);
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
        var support = hibernation;
        if (support != null) {
            support.stop();
        }
        var executions = workflowExecutionRepository.findAll();
        logger.info("Shutting down WorkflowEngine: interrupting running steps of {} workflow instance(s).",
                    executions.size());
        for (var execution : executions) {
            execution.stopForShutdown();
        }
        workflowExecutionRepository.clear();
        checkpointWorkIndex.clear();
        registeredCancellations.clear();
        workflowCancellationService.clear();
        executions.forEach(execution -> leftMemory(execution, null));
    }

    /**
     * Narrows the unsafe-execution index to the instances owned by the given segment. A segment token only covers the
     * instances that segment owns, so executions of other segments must not hold it back, and their index entries must
     * not be marked safe on its behalf either.
     */
    private Stream<String> unsafeWorkflowIdsOf(Segment segment) {
        return checkpointWorkIndex.unsafeWorkflowIdsOf(segment).stream();
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
