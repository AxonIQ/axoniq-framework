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
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngineHibernationSupport.SegmentClaim;
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

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState.PAYLOAD_TYPE;
import static io.axoniq.framework.workflow.runtime.execution.NewWorkflowInstanceRouting.hasDerivedWorkflowId;
import static io.axoniq.framework.workflow.runtime.execution.NewWorkflowInstanceRouting.resolveWorkflowIdForNewInstance;
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
    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngine.class);
    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private final WorkflowExecutionRepository workflowExecutionRepository;
    private final WorkflowCancellationService workflowCancellationService;
    private final WorkflowStore workflowStore;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final WorkflowEngineCatchUpSupport catchUpSupport = new WorkflowEngineCatchUpSupport();
    private final WorkflowEngineSequencingPolicy segmentedRouting;
    private final UnsafeCheckpointWorkIndex checkpointWorkIndex = new UnsafeCheckpointWorkIndex();
    private final WaitAssociationIndex waitIndex;
    // Package-private so a test can shrink it instead of waiting out the production timeout.
    Duration restoreTimeout = DEFAULT_RESTORE_TIMEOUT;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;
    // Package-private so a test can run a hibernation pass instead of waiting for the interval.
    @Nullable WorkflowEngineHibernationSupport hibernation;
    private @Nullable WorkflowEngineIndexSupport index;

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
        this.waitIndex = new WaitAssociationIndex(this::tracksWaitsOf);
        this.segmentedRouting = new WorkflowEngineSequencingPolicy(
                workflowConfigurationRegistry, waitIndex, catchUpSupport::isCaughtUp
        );
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
     * Enables hibernation: a workflow that only waits for an external event or a deadline is removed from memory, and
     * restored from the event store when that event arrives or that deadline expires.
     * <p>
     * Set outside the constructor, like {@link #setCheckpointingSupport(WorkflowEngineCheckpointingSupport)}, by the
     * workflow module when its hibernation setting is enabled. The {@link WorkflowEngineHibernationSupport} owns the
     * bookkeeping; this engine only hands it events, cancellations and executions at a few hooks.
     *
     * @param scheduler    schedules the passes and the deadlines of hibernated workflows
     * @param clock        the clock the scheduler measures deadlines with
     * @param interval     the time between 2 passes, and before a failed wake is tried again
     * @param wakeExecutor runs the wakes for a deadline or a cancellation, like the workflows of this engine
     */
    @Internal
    public void enableHibernation(WorkflowScheduler scheduler,
                                  Clock clock,
                                  Duration interval,
                                  Executor wakeExecutor) {
        this.hibernation = new WorkflowEngineHibernationSupport(
                scheduler, clock, workflowExecutionRepository, workflowCancellationService, checkpointWorkIndex,
                unitOfWorkFactory, () -> restoreTimeout, this::loadWorkflow, interval, wakeExecutor, () -> index
        );
    }

    /**
     * Enables the given {@link WorkflowIndex}, so a claimed segment reads its workflows and waits from the rows of the
     * index instead of sourcing them. Internal, because the workflow module calls it when an index is configured.
     *
     * @param workflowIndex the index to keep current and to claim from
     * @param name          the name of the workflow module, which keeps its rows apart from those of other modules
     */
    @Internal
    public void enableIndex(WorkflowIndex workflowIndex, String name) {
        this.index = new WorkflowEngineIndexSupport(workflowIndex, name, unitOfWorkFactory);
    }

    @Override
    public MessageStream.Empty<Message> handle(EventMessage event,
                                               ProcessingContext context) {
        TrackingToken currentToken = TrackingToken.fromContext(context).orElse(null);
        logger.trace("Handling event [{}] with id [{}] and token [{}].",
                     event.type(), event.identifier(), currentToken);
        checkpointingSupport.getAndSetTriggerFrom(context);

        var segment = Segment.fromContext(context).orElse(null);
        // An event published by a workflow through the publish primitive carries the publisher's workflowId but is a
        // business event for everyone else, so it never takes the owner-only path below.
        boolean workflowPublishedEvent = MetadataUtils.isPublishStep(event.metadata());
        if (!workflowPublishedEvent && MetadataUtils.hasWorkflowId().test(event.metadata())) {
            var workflowId = MetadataUtils.getWorkflowId(event.metadata());
            // Instance partitioning: a segment only processes instances it owns.
            if (!WorkflowSegmentOwnership.ownedBy(segment, workflowId)) {
                logger.debug("Ignoring event [{}] for workflowId [{}] - instance is owned by another segment than {}.",
                             event.type(), workflowId, segment);
                activateSegmentWhenCaughtUp(segment, currentToken);
                checkpointingSupport.requestCheckpoint(segment, currentToken);
                return MessageStream.empty();
            }
            if (index != null) {
                index.record(segment, currentToken, event);
            }
            // Skip events whose workflowId isn't owned by this engine (expected in multi-module setups).
            if (!deliver(workflowId, event, context)) {
                logger.debug("Ignoring event [{}] for workflowId [{}] - no matching execution in this engine.",
                             event.type(), workflowId);
                activateSegmentWhenCaughtUp(segment, currentToken);
                checkpointingSupport.requestCheckpoint(segment, currentToken);
                return MessageStream.empty();
            }
        } else {
            // handle starting of new processes. Events published by a workflow through the publish primitive take
            // this branch as well: they may start or wake other instances, and the publishing instance must receive
            // its own event, which records the published step and unblocks the publisher.
            var created = checkAndCreateNewInstance(event, context);
            deliverToWaitingWorkflows(event, context, segment, workflowPublishedEvent, created);
        }

        activateSegmentWhenCaughtUp(segment, currentToken);
        checkpointingSupport.requestCheckpoint(segment, currentToken);
        logger.trace("Event [{}] handled successfully.", event.identifier());
        return MessageStream.empty();
    }

    /**
     * Delivers the event to the workflows of this segment waiting for it, plus the instances it just created and, for a
     * published event, its publisher.
     */
    private void deliverToWaitingWorkflows(EventMessage event,
                                           ProcessingContext context,
                                           @Nullable Segment segment,
                                           boolean workflowPublishedEvent,
                                           Set<String> created) {
        Set<String> workflowIds = new LinkedHashSet<>(waitIndex.match(event, context));
        workflowIds.addAll(created);
        if (workflowPublishedEvent) {
            workflowIds.add(MetadataUtils.getWorkflowId(event.metadata()));
        }
        for (var workflowId : workflowIds) {
            if (WorkflowSegmentOwnership.ownedBy(segment, workflowId)) {
                deliver(workflowId, event, context);
            }
        }
    }

    /**
     * Delivers the event to the execution of the given workflow, or hands it to the hibernation support, which also
     * finds a hibernated workflow.
     *
     * @return {@code false} when this engine runs no such workflow, which is expected in multi-module setups
     */
    private boolean deliver(String workflowId, EventMessage event, ProcessingContext context) {
        if (hibernation != null) {
            return hibernation.deliver(workflowId, event, context);
        }
        var executionOpt = workflowExecutionRepository.findById(workflowId);
        executionOpt.ifPresent(execution -> execution.onEvent(event, context));
        return executionOpt.isPresent();
    }

    private Set<String> checkAndCreateNewInstance(EventMessage eventMessage,
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
        var created = new HashSet<String>();

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
            var workflowId = resolveWorkflowIdForNewInstance(
                    workflowExecutionRepository, baseWorkflowId, workflowConfiguration.workflowVersion(), eventMessage
            );

            if (workflowId == null) {
                return;
            }
            if (hibernation != null && hibernation.isDuplicateStart(workflowId)) {
                logger.warn("A workflow with id '{}' is already running; ignoring the new start request. Workflow ids "
                                    + "MUST be unique, so the start is treated as a duplicate.", workflowId);
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
            registerCheckpointWork(execution);
            created.add(execution.workflowId());
            if (isSegmentCaughtUp(segment)) {
                execute(execution, segment);
            }
        });
        return created;
    }

    @Override
    public boolean hasUnsafeCheckpointWork(Segment segment) {
        return unsafeExecutionsOf(segment).anyMatch(WorkflowExecution::isRunning);
    }

    @Override
    public boolean holdsCheckpoint(Segment segment) {
        return unsafeExecutionsOf(segment).anyMatch(execution -> !execution.isRunning());
    }

    @Override
    public CompletableFuture<Void> onCheckpointStore(Segment segment,
                                                     TrackingToken token,
                                                     ProcessingContext processingContext) {
        var support = index;
        return support == null
                ? CompletableFuture.completedFuture(null)
                : support.store(segment, token, processingContext);
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
        if (hibernation != null) {
            hibernation.start();
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
     * The instances are materialized from their current durable state and started immediately. With hibernation, a
     * workflow whose {@link WorkflowIndex} row carries a wake time and whose open waits did not change since stays out
     * of memory instead.
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
        // Tags the restored executions with their segment, so that a completion arriving long after this callback
        // returned is attributed to the segment that owns the instance instead of being dropped without one. Only
        // safe because the start pass below is scoped to this segment: every body started here is owned by it.
        executionContext.putResource(Segment.RESOURCE_KEY, segment);
        if (claimedFrom != null) {
            executionContext.putResource(TrackingToken.RESOURCE_KEY, claimedFrom);
        }
        // A split or a merge replaces segments without releasing them, so a reused segment id catches up anew.
        catchUpSupport.release(segment);
        // The segment's events are broadcast until its waits are loaded.
        waitIndex.segmentClaimed(segment);
        var claim = hibernation == null ? null : hibernation.claiming(segment);
        // The waits are indexed last, as the index keeps the rows of the workflows this engine holds.
        return claimedWorkflows(segment, claimedFrom, sourcingContext)
                .thenCompose(claimed -> restoreRunningWorkflows(
                        segment,
                        claimed.runningWorkflowIds(),
                        sourcingContext,
                        executionContext,
                        adoptsAsHibernated(segment, claimed.hibernated(), claimed.waits(), claim)
                ).thenRun(() -> {
                    indexWaits(segment, claimed.waits());
                    if (hibernation != null && claim != null) {
                        hibernation.claimed(segment, claim);
                    }
                }))
                .orTimeout(restoreTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((restored, failure) -> logFailedRestore(segment, failure))
                .thenRun(() -> startRestoredWorkflowsWhenCaughtUp(segment, claimedFrom));
    }

    /**
     * Returns what the claim of the given segment restores from: the rows of the {@link WorkflowIndex} when they can
     * answer it, otherwise what the {@link WorkflowStore} sources.
     */
    private CompletableFuture<ClaimedWorkflows> claimedWorkflows(Segment segment,
                                                                 @Nullable TrackingToken claimedFrom,
                                                                 ProcessingContext sourcingContext) {
        var support = index;
        if (support == null) {
            return sourceClaimedWorkflows(segment, sourcingContext);
        }
        return support.claim(segment, claimedFrom, sourcingContext).thenCompose(
                rows -> rows.map(claimed -> CompletableFuture.completedFuture(new ClaimedWorkflows(
                                    claimed.runningWorkflowIds(),
                                    claimed.waits(),
                                    support.keepsWakeTimes() ? claimed.hibernated() : HibernatedWorkflows.NONE
                            )))
                            .orElseGet(() -> sourceClaimedWorkflows(segment, sourcingContext))
        );
    }

    /**
     * Sources what the claim of the given segment restores from, with no workflow hibernated: only index rows know when
     * one wakes. With a {@link WorkflowIndex}, the result becomes the rows of the segment.
     */
    private CompletableFuture<ClaimedWorkflows> sourceClaimedWorkflows(Segment segment,
                                                                       ProcessingContext sourcingContext) {
        var support = index;
        return workflowStore.loadWaitAssociations(sourcingContext).thenCompose(
                waits -> workflowStore.loadRunningWorkflows(sourcingContext).thenApply(running -> {
                    if (support != null) {
                        support.sourced(segment, running.workflowIds(), waits.waitAssociations());
                    }
                    return new ClaimedWorkflows(
                            running.workflowIds(),
                            waits.waitAssociations()
                                 .stream()
                                 .filter(wait -> WorkflowSegmentOwnership.ownedBy(segment, wait.workflowId()))
                                 .toList(),
                            HibernatedWorkflows.NONE
                    );
                })
        );
    }

    private void indexWaits(Segment segment, List<WaitAssociation> waits) {
        waitIndex.segmentLoaded(segment, waits);
        logger.debug("Loaded {} open wait(s) of segment {}.", waits.size(), segment.getSegmentId());
    }

    /**
     * What the claim of a segment restores from.
     *
     * @param runningWorkflowIds the identifiers of running workflows, at least those of the segment
     * @param waits              the open waits of the segment
     * @param hibernated         the workflows the rows of the segment keep hibernated
     */
    private record ClaimedWorkflows(Collection<String> runningWorkflowIds,
                                    List<WaitAssociation> waits,
                                    HibernatedWorkflows hibernated) {

    }

    /**
     * Returns whether the claim of the given segment adopts a running workflow as hibernated instead of restoring it:
     * its row carries a wake time, and its open waits are exactly the ones it hibernated with.
     */
    private Predicate<String> adoptsAsHibernated(Segment segment,
                                                 HibernatedWorkflows recorded,
                                                 List<WaitAssociation> waits,
                                                 @Nullable SegmentClaim claim) {
        var support = hibernation;
        if (support == null || claim == null || recorded.workflows().isEmpty()) {
            return workflowId -> false;
        }
        var openWaits = waits.stream().collect(Collectors.groupingBy(
                WaitAssociation::workflowId, Collectors.mapping(WaitAssociation::stepName, Collectors.toSet())
        ));
        return workflowId -> recorded.hibernated(workflowId, openWaits.getOrDefault(workflowId, Set.of()))
                                     .map(workflow -> support.adopt(workflowId, segment, workflow, claim))
                                     .orElse(false);
    }

    /**
     * Whether the waits of the given workflow belong in the {@link WaitAssociationIndex}: it runs in this engine and
     * has not reached a terminal status, or it is hibernated here.
     */
    private boolean tracksWaitsOf(String workflowId) {
        return (hibernation != null && hibernation.isHibernated(workflowId))
                || workflowExecutionRepository.findById(workflowId)
                                              .filter(execution -> !execution.state().workflowStatus().isTerminal())
                                              .isPresent();
    }

    /**
     * Drops the workflow executions owned by a segment this node no longer holds.
     * <p>
     * In-flight steps are stopped rather than cancelled, exactly as on {@link #shutdown()}: no cancellation events are
     * emitted, so the node claiming the segment next resumes each instance from its persisted state. With hibernation,
     * the segment's hibernated workflows are forgotten, and a {@link WorkflowIndex} keeps them out of memory.
     *
     * @param segment the segment that was released
     * @return a future completed after this engine has requested every released execution to stop
     */
    public CompletableFuture<Void> releaseWorkflowsFor(Segment segment) {
        waitIndex.segmentReleased(segment);
        if (hibernation != null) {
            hibernation.release(segment);
        }
        // Also without executions in memory: the next claim of the segment must catch up and start its restored ones.
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
            return;
        }
        removeTerminalAndStartRestoredWorkflowExecutions(segment, "on claim of segment " + segment.getSegmentId());
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

    private CompletableFuture<Void> restoreRunningWorkflows(Segment segment,
                                                            Collection<String> runningWorkflowIds,
                                                            ProcessingContext sourcingContext,
                                                            ProcessingContext executionContext,
                                                            Predicate<String> adoptsAsHibernated) {
        var ownedIds = runningWorkflowIds.stream()
                                         .filter(id -> WorkflowSegmentOwnership.ownedBy(segment, id))
                                         .filter(adoptsAsHibernated.negate())
                                         .toList();
        if (index != null) {
            ownedIds.forEach(workflowId -> index.restored(segment, workflowId));
        }
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
        registerCheckpointWork(storedExecution);
    }

    /**
     * Loads a hibernated workflow back from the event store and starts it, as the hibernation support's
     * {@link WorkflowEngineHibernationSupport.WorkflowLoader}. The given action runs on the loaded execution before its
     * body starts.
     */
    private void loadWorkflow(String workflowId,
                              ProcessingContext executionContext,
                              @Nullable Segment segment,
                              Consumer<WorkflowExecution> beforeStart) {
        restoreWorkflow(workflowId, executionContext, executionContext)
                .orTimeout(restoreTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .join();
        var execution = workflowExecutionRepository.findById(workflowId).orElseThrow();
        if (execution.state().workflowStatus().isTerminal()) {
            // Another node ended it while it hibernated here: forget it, as a claim does.
            removeExecution(workflowId);
            logger.debug("Workflow [{}] ended elsewhere while it hibernated here.", workflowId);
            return;
        }
        beforeStart.accept(execution);
        // A workflow adopted by a claim can wake while its segment catches up. It then starts with the other restored
        // executions once the segment caught up, like them.
        if (isSegmentCaughtUp(segment)) {
            registerCancellation(execution);
            execute(execution, segment);
        }
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
        if (index != null) {
            // A row that outlived its workflow would be restored by every claim.
            terminalExecutions.forEach(execution -> index.ended(segment, execution.workflowId()));
        }
        var executionsToStart = workflowExecutionRepository.findAll(owned.and(execution -> !execution.isRunning()));
        // From here on, a woken workflow of this segment starts itself, as it is not among the executions started here.
        if (hibernation != null) {
            hibernation.arm(segment);
        }
        if (executionsToStart.isEmpty()) {
            logger.debug("No restored workflow executions require startup {}.", phase);
            return;
        }
        logger.debug("Starting {} restored workflow execution(s) {}.", executionsToStart.size(), phase);
        for (var execution : executionsToStart) {
            registerCancellation(execution);
            execute(execution, segment);
        }
        logger.debug("Started {} restored workflow execution(s) {}.", executionsToStart.size(), phase);
    }

    private void execute(WorkflowExecution execution, @Nullable Segment segment) {
        execution.workflowExecutionOperations()
                 .processingContext()
                 .whenComplete(context -> {
                     try {
                         var executionToken = TrackingToken.fromContext(context).orElse(null);
                         logger.debug("Executing workflow execution with id: {}", execution.workflowId());
                         execution.execute(
                                 finished -> {
                                     logger.debug("Workflow {} finished with status {}, removing it from repository",
                                                  execution.workflowId(),
                                                  finished.state().workflowStatus());
                                     removeExecution(execution.workflowId());
                                     // The completion is safe for the segment that owns this instance and started its
                                     // body, at that segment's own position: never at another segment's.
                                     checkpointingSupport.requestCheckpoint(segment, executionToken);
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
            var cancellation = provider.workflowCancellation();
            workflowCancellationService.register(
                    execution.workflowId(),
                    hibernation == null ? cancellation : hibernation.decorate(execution, cancellation)
            );
        } else {
            throw new IllegalStateException(
                    "Provided workflow execution {} does not implement WorkflowCancellationProvider, but the cancellation was registered.".formatted(
                            execution.workflowId()));
        }
    }

    /**
     * Lets the checkpoint work index follow the given execution, and lets the execution hibernate when hibernation is
     * enabled.
     */
    private void registerCheckpointWork(WorkflowExecution execution) {
        var support = hibernation;
        if (support == null) {
            checkpointWorkIndex.register(execution.workflowId(), execution::registerCheckpointWorkStateListener);
            return;
        }
        checkpointWorkIndex.register(
                execution.workflowId(),
                listener -> execution.registerCheckpointWorkStateListener(support.decorate(execution, listener))
        );
        support.allow(execution);
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
     * A workflow driver that exits after this call skips its termination handler, so it touches no engine state and
     * requests no checkpoint.
     */
    public void shutdown() {
        if (hibernation != null) {
            hibernation.stop();
        }
        var executions = workflowExecutionRepository.findAll();
        logger.info("Shutting down WorkflowEngine: interrupting running steps of {} workflow instance(s).",
                    executions.size());
        for (var execution : executions) {
            execution.stopForShutdown();
        }
        workflowExecutionRepository.clear();
        checkpointWorkIndex.clear();
        waitIndex.clear();
        if (index != null) {
            index.clear();
        }
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
