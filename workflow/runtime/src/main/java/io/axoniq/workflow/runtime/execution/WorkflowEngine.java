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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.axoniq.workflow.runtime.execution.EventSourcedWorkflowState.PAYLOAD_TYPE;
import static io.axoniq.workflow.runtime.execution.NewWorkflowInstanceRouting.hasDerivedWorkflowId;
import static io.axoniq.workflow.runtime.execution.NewWorkflowInstanceRouting.resolveWorkflowIdForNewInstance;
import static java.util.Objects.requireNonNull;

/**
 * Main workflow component responsible for managing and executing workflows.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 1.0.0
 */
@Internal
public class WorkflowEngine implements
        EventHandler,
        WorkflowEngineReplaySupport.LiveModeActivatedCallback,
        WorkflowEngineCheckpointingSupport.CheckpointLatchCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngine.class);

    private static final String AFTER_REPLAY_LOG = "after replay catch-up";

    /**
     * Time {@link #restoreWorkflowsFor(Segment, TrackingToken, ProcessingContext, ProcessingContext)} is given to load
     * a segment's durable state before it fails the claim.
     * <p>
     * The workflow store is remote, so the load can stall without failing. The processor logs a failing claim listener
     * and carries on, so an unbounded load would leave the segment claimed here with none of its instances running. A
     * release needs no such bound: the processor already caps it at its claim extension threshold.
     */
    static final Duration DEFAULT_RESTORE_TIMEOUT = Duration.ofSeconds(30);

    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private final WorkflowExecutionRepository workflowExecutionRepository;
    private final WorkflowCancellationService workflowCancellationService;
    private final WorkflowStore workflowStore;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private WorkflowEngineReplaySupport replaySupport;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;
    private final WorkflowEngineSequencingPolicy segmentedRouting;
    private final UnsafeCheckpointWorkIndex checkpointWorkIndex = new UnsafeCheckpointWorkIndex();
    // Package-private so a test can shrink it instead of waiting out the production timeout.
    Duration restoreTimeout = DEFAULT_RESTORE_TIMEOUT;

    /**
     * Creates a new workflow engine. Be sure to follow-up construction of a {@code WorkflowEngine} with an invocation
     * of {@link #setEngineSupportComponents(WorkflowEngineReplaySupport, WorkflowEngineCheckpointingSupport)}, as
     * otherwise replay and checkpointing support is unavailable.
     *
     * @param workflowConfigurationRegistry registry containing a
     *                                      {@link
     *                                      io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration
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
     * Sets {@code WorkflowEngine} support components which are <b>required</b> for the engine to work.
     * <p>
     * Both {@code replaySupport} and {@code checkpointingSupport} are set outside the
     * {@link #WorkflowEngine(WorkflowConfigurationRegistry, WorkflowExecutionRepository, WorkflowCancellationService,
     * WorkflowStore, UnitOfWorkFactory)}, because they require <b>this</b> {@code WorkflowEngine} itself to function.
     * Hence, a cyclic dependency would exist upon start-up if completion otherwise.
     *
     * @param replaySupport        provides replayability support to this {@code WorkflowEngine}
     * @param checkpointingSupport provides checkpointing support to this {@code WorkflowEngine}
     */
    @Internal
    public void setEngineSupportComponents(WorkflowEngineReplaySupport replaySupport,
                                           WorkflowEngineCheckpointingSupport checkpointingSupport) {
        this.replaySupport = requireNonNull(replaySupport, "The WorkflowEngineReplaySupport must not be null.");
        this.checkpointingSupport = requireNonNull(
                checkpointingSupport, "The WorkflowEngineCheckpointingSupport must not be null."
        );
    }

    @Override
    public MessageStream.Empty<Message> handle(EventMessage event,
                                               ProcessingContext context) {
        TrackingToken currentToken = replaySupport.getAndSetTokenFrom(context);
        logger.trace("Handling event [{}] with id [{}] and token [{}].",
                     event.type(), event.identifier(), currentToken);
        checkpointingSupport.getAndSetTriggerFrom(context);

        var segment = Segment.fromContext(context).orElse(null);
        if (MetadataUtils.hasWorkflowId().test(event.metadata())) {
            var workflowId = MetadataUtils.getWorkflowId(event.metadata());
            // Instance partitioning: a segment only processes instances it owns.
            if (!WorkflowSegmentOwnership.ownedBy(segment, workflowId)) {
                logger.debug("Ignoring event [{}] for workflowId [{}] - instance is owned by another segment than {}.",
                             event.type(), workflowId, segment);
                checkpointingSupport.requestCheckpoint(segment, currentToken);
                replaySupport.validateIfReplayFinished(currentToken, context);
                return MessageStream.empty();
            }
            // Skip events whose workflowId isn't owned by this engine (expected in multi-module setups).
            var executionOpt = workflowExecutionRepository.findById(workflowId);
            if (executionOpt.isEmpty()) {
                logger.debug("Ignoring event [{}] for workflowId [{}] - no matching execution in this engine.",
                             event.type(), workflowId);
                checkpointingSupport.requestCheckpoint(segment, currentToken);
                replaySupport.validateIfReplayFinished(currentToken, context);
                return MessageStream.empty();
            }
            executionOpt.get().onEvent(event, context);
        } else {
            // handle starting of new processes
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

        checkpointingSupport.requestCheckpoint(segment, currentToken);
        replaySupport.validateIfReplayFinished(currentToken, context);
        logger.trace("Event [{}] handled successfully.", event.identifier());
        return MessageStream.empty();
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
            var workflowId = resolveWorkflowIdForNewInstance(
                    workflowExecutionRepository, baseWorkflowId, workflowConfiguration.workflowVersion(), eventMessage
            );

            if (workflowId == null) {
                return;
            }

            var payload = requireNonNull(eventMessage.payloadAs(PAYLOAD_TYPE), "Error converting initial payload");
            var workflowContext = workflowConfiguration.workflowContextFactory().createContext(
                    payload, workflowId, processingContext, workflowConfiguration
            );

            var execution = workflowExecutionRepository.save(workflowId, () -> {
                logger.debug("Creating a new workflow '{}' with payload '{}'", workflowId, eventMessage.payload());
                return workflowConfiguration.workflowExecutionFactory().create(workflowContext);
            });
            registerCancellation(execution);
            checkpointWorkIndex.register(execution.workflowId(), execution::registerCheckpointWorkStateListener);
            if (replaySupport.inLiveMode(segment)) {
                execute(execution, segment);
            }
        });
    }

    @Override
    public void invoke(ProcessingContext context) {
        this.workflowConfigurationRegistry.warnAboutSameVersionDuplicates();
        logger.info("Workflow instance replay finished. Switching to live mode.");
        // Segments catch up independently, so this switch is per segment: it starts the executions of the segment
        // that just caught up, never those of a segment still behind.
        removeTerminalAndStartRestoredWorkflowExecutions(Segment.fromContext(context).orElse(null),
                                                         AFTER_REPLAY_LOG);
    }

    @Override
    public boolean hasUnsafeCheckpointWork(Segment segment) {
        return unsafeWorkflowIdsOf(segment).findAny().isPresent();
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
                                executions.add(execution);
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
     * Initializes replay tracking before processor replay resumes.
     * <p>
     * Workflow executions are not restored here: they are restored per segment by
     * {@link #restoreWorkflowsFor(Segment, TrackingToken, ProcessingContext, ProcessingContext)} as this node claims
     * them.
     *
     * @param processorToken processor token at startup; initializes replay tracking when no processor token has been
     *                       observed yet
     * @param replayRequired whether the processor must catch up before the engine goes live
     * @return a future that completes when the engine is in the required replay or live mode
     */
    public CompletableFuture<Void> start(@Nullable TrackingToken processorToken, boolean replayRequired) {
        return unitOfWorkFactory.create("WorkflowRehydration").executeWithResult(
                sourcingContext -> {
                    replaySupport.setCurrentTokenIfNull(processorToken);
                    UnitOfWork executionUnitOfWork = new SimpleUnitOfWorkFactory(sourcingContext).create();

                    return executionUnitOfWork.executeWithResult(
                            executionContext -> {
                                replaySupport.initializeRestoreProcessingContext(null, null, executionContext);
                                if (!replayRequired) {
                                    replaySupport.switchToLiveMode(sourcingContext);
                                }
                                return CompletableFuture.<Void>completedFuture(null);
                            }
                    );
                }
        );
    }

    /**
     * Restores and starts the workflow executions owned by a segment this node just claimed.
     * <p>
     * Restoration is per segment rather than per node: a node materializes only the instances of the segments it holds,
     * so a segment migrating between nodes carries its instances with it. When another node dies, the coordinator hands
     * its segments to a surviving node and this callback rebuilds their instances there, without restarting anything.
     * <p>
     * The instances are materialized from their current durable state, which is the state at the end of the stream, not
     * at the segment's position. Running their bodies while the segment is still replaying would let them act on events
     * the segment has not delivered yet, so a segment known to be behind only materializes here; its bodies start when
     * that segment catches up.
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
        return loadRunningWorkflows(segment, claimedFrom, sourcingContext, executionContext)
                .orTimeout(restoreTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((restored, failure) -> logFailedRestore(segment, failure))
                .thenRun(() -> startRestoredWorkflowsUnlessReplaying(segment, claimedFrom));
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
        var released = workflowExecutionRepository.findAll(ownedBy(segment));
        if (released.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        logger.info("Releasing segment {}: stopping {} workflow execution(s).",
                    segment.getSegmentId(), released.size());
        released.forEach(WorkflowExecution::stopForShutdown);
        workflowExecutionRepository.removeAll(ownedBy(segment));
        return CompletableFuture.completedFuture(null);
    }

    private void startRestoredWorkflowsUnlessReplaying(Segment segment, @Nullable TrackingToken claimedFrom) {
        if (replaySupport.isReplaying(segment, claimedFrom)) {
            logger.info("Segment {} is still replaying: restored workflow executions start once it catches up.",
                        segment.getSegmentId());
            return;
        }
        removeTerminalAndStartRestoredWorkflowExecutions(segment, "on claim of segment " + segment.getSegmentId());
    }

    private void logFailedRestore(Segment segment, @Nullable Throwable failure) {
        if (failure != null) {
            logger.error("Restoring the workflow executions of segment {} did not complete within {}; failing the "
                                 + "claim so the coordinator can hand the segment out again.",
                         segment.getSegmentId(), restoreTimeout, failure);
        }
    }

    private Predicate<WorkflowExecution> ownedBy(@Nullable Segment segment) {
        return execution -> WorkflowSegmentOwnership.ownedBy(segment, execution.workflowId());
    }

    private CompletableFuture<Void> loadRunningWorkflows(Segment segment,
                                                         @Nullable TrackingToken claimedFrom,
                                                         ProcessingContext sourcingContext,
                                                         ProcessingContext executionContext) {
        replaySupport.initializeRestoreProcessingContext(segment, claimedFrom, executionContext);
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
                                                           .map(workflowId -> workflowStore
                                                                   .loadWorkflow(workflowId, sourcingContext)
                                                                   .thenAccept(state -> restoreWorkflow(
                                                                           workflowId,
                                                                           state,
                                                                           executionContext
                                                                   ))
                                                                   // Per instance, so one unrestorable workflow cannot
                                                                   // abort the whole restore pass. Aborting it would
                                                                   // fail the segment claim callback, which the
                                                                   // processor only logs, leaving every other instance
                                                                   // of that segment permanently unrehydrated while the
                                                                   // node looks healthy. The skipped instance keeps its
                                                                   // durable state and is restored by a later claim.
                                                                   .exceptionally(failure -> {
                                                                       logger.error(
                                                                               "Skipping workflow '{}' of segment {}: "
                                                                                       + "it could not be restored. It "
                                                                                       + "stays durable and is not "
                                                                                       + "running on this node.",
                                                                               workflowId,
                                                                               segment.getSegmentId(),
                                                                               failure);
                                                                       return null;
                                                                   }))
                                                           .toArray(CompletableFuture[]::new);
                                return CompletableFuture.allOf(rehydrations);
                            });
    }

    private void restoreWorkflow(String workflowId,
                                 WorkflowState state,
                                 ProcessingContext executionContext) {
        var definitionId = state.workflowDefinitionId();
        var workflowName = definitionId.qualifiedName().toString();
        // Same routing as the replay path: a body may have moved its recorded version forward with
        // ctx.migrateVersion(...) to a version no definition is statically registered under, so an exact lookup alone
        // cannot restore it.
        var workflowConfiguration = workflowConfigurationRegistry
                .getWorkflowConfiguration(definitionId)
                .or(() -> workflowConfigurationRegistry.findClosestRegisteredVersion(workflowName,
                                                                                     definitionId.version()))
                .or(() -> workflowConfigurationRegistry.findClosestHigherRegisteredVersion(workflowName,
                                                                                           definitionId.version()))
                .orElseThrow(() -> new IllegalStateException(
                        "No workflow configuration found for workflow '%s' with definition %s."
                                .formatted(workflowId, definitionId)
                ));
        var workflowContext = workflowConfiguration.workflowContextFactory().createContext(
                state.payload(),
                workflowId,
                executionContext,
                workflowConfiguration
        );
        var execution = workflowConfiguration.workflowExecutionFactory().create(workflowContext);
        execution.initializeState(state);
        var storedExecution = workflowExecutionRepository.save(workflowId, () -> execution);
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
            logger.info("No restored workflow executions require startup {}.", phase);
            return;
        }
        logger.info("Starting {} restored workflow execution(s) {}.", executionsToStart.size(), phase);
        for (var execution : executionsToStart) {
            registerCancellation(execution);
            execute(execution, segment);
        }
        logger.info("Started {} restored workflow execution(s) {}.", executionsToStart.size(), phase);
    }

    private void execute(WorkflowExecution execution, @Nullable Segment segment) {
        execution.workflowContext()
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
                                     checkpointWorkIndex.markSafe(execution.workflowId());
                                     // The completion is safe for the segment that owns this instance and started its
                                     // body, at that segment's own position: never at another segment's.
                                     checkpointingSupport.requestCheckpoint(segment,
                                                                            replaySupport.currentToken(segment));
                                 }
                         );
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

    private void removeExecution(String workflowId) {
        workflowExecutionRepository.remove(workflowId);
        workflowCancellationService.unregister(workflowId);
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
     */
    public void shutdown() {
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
