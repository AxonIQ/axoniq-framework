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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.workflow.runtime.execution.EventSourcedWorkflowState.PAYLOAD_TYPE;
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
        WorkflowEngineCheckpointingSupport.CheckpointBarrierCoordinator,
        WorkflowEngineReplaySupport.LiveModeActivatedCallback {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngine.class);

    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private final WorkflowExecutionRepository workflowExecutionRepository;
    private final WorkflowStore workflowStore;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final WorkflowEngineReplaySupport replaySupport;
    private final WorkflowEngineCheckpointingSupport checkpointingSupport;

    private final WorkflowEngineCheckpointWorkIndex checkpointWorkIndex = new WorkflowEngineCheckpointWorkIndex();

    /**
     * Creates a new workflow engine.
     *
     * @param workflowConfigurationRegistry registry containing a
     *                                      {@link
     *                                      io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration
     *                                      workflow configuration}
     * @param workflowExecutionRepository   repository dedicated towards {@link WorkflowExecution} storage and
     *                                      retrieval
     * @param workflowStore                 dedicated store for workflow information, like {@link RunningWorkflows} and
     *                                      {@link WorkflowState}
     * @param unitOfWorkFactory             a unit of work factory dedicated to construct a unit of work during
     *                                      {@link #start(TrackingToken, boolean)} of this engine
     * @param replaySupport                 provides replayability support to this {@code WorkflowEngine}
     * @param checkpointingSupport          provides checkpointing support to this {@code WorkflowEngine}
     */
    public WorkflowEngine(
            @Nonnull WorkflowConfigurationRegistry<?> workflowConfigurationRegistry,
            @Nonnull WorkflowExecutionRepository workflowExecutionRepository,
            @Nonnull WorkflowStore workflowStore,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull WorkflowEngineReplaySupport replaySupport,
            @Nonnull WorkflowEngineCheckpointingSupport checkpointingSupport
    ) {
        this.workflowConfigurationRegistry = requireNonNull(
                workflowConfigurationRegistry, "The WorkflowConfigurationRegistry must not be null."
        );
        this.workflowExecutionRepository = requireNonNull(
                workflowExecutionRepository, "The WorkflowExecutionRepository must not be null."
        );
        this.workflowStore = requireNonNull(workflowStore, "The WorkflowStore must not be null.");
        this.unitOfWorkFactory = requireNonNull(unitOfWorkFactory, "The UnitOfWorkFactory must not be null.");
        this.replaySupport = requireNonNull(replaySupport, "The WorkflowEngineReplaySupport must not be null.");
        this.checkpointingSupport = requireNonNull(
                checkpointingSupport, "The WorkflowEngineCheckpointingSupport must not be null."
        );
        EntitlementManager.INSTANCE.registerAddon(WorkflowAxoniqAddon.class);
    }

    @Nonnull
    @Override
    public MessageStream.Empty<Message> handle(@Nonnull EventMessage eventMessage,
                                               @Nonnull ProcessingContext processingContext) {
        var currentTrackingToken = replaySupport.observeProcessingContext(processingContext);
        checkpointingSupport.observeProcessingContext(processingContext);
        logger.trace("Received eventMessage {} {} {}",
                     processingContext.resources().get(TrackingToken.RESOURCE_KEY),
                     eventMessage.identifier(),
                     eventMessage.type()
        );
        if (MetadataUtils.hasWorkflowId().test(eventMessage.metadata())) {
            var workflowId = MetadataUtils.getWorkflowId(eventMessage.metadata());
            // Skip events whose workflowId isn't owned by this engine (expected in multi-module setups).
            var executionOpt = workflowExecutionRepository.findById(workflowId);
            if (executionOpt.isEmpty()) {
                logger.debug("Ignoring event {} for workflowId '{}' — no matching execution in this engine.",
                             eventMessage.type(), workflowId);
                checkpointingSupport.requestCheckpoint(currentTrackingToken);
                replaySupport.advanceReplayPosition(currentTrackingToken, processingContext);
                return MessageStream.empty();
            }
            executionOpt.get().onEvent(eventMessage, processingContext);
        } else {
            // handle starting of new processes
            checkAndCreateNewWorkflow(eventMessage, processingContext);
            // route external events to workflows waiting for them
            for (var execution : workflowExecutionRepository.findAll()) {
                execution.onEvent(eventMessage, processingContext);
            }
        }

        checkpointingSupport.requestCheckpoint(currentTrackingToken);
        replaySupport.advanceReplayPosition(currentTrackingToken, processingContext);
        logger.trace("EventMessage {} successfully handled", eventMessage.identifier());
        return MessageStream.empty();
    }

    private void execute(@Nonnull WorkflowExecution execution, @Nonnull ProcessingContext processingContext) {
        var replaySupport = processingContext.component(WorkflowEngineReplaySupport.class);
        var checkpointingSupport = processingContext.component(WorkflowEngineCheckpointingSupport.class);

        execution
                .workflowContext()
                .processingContext()
                .whenComplete(pc -> {
                    try {
                        logger.debug("Executing workflow execution with id: {}", execution.workflowId());
                        execution.execute(
                                finished -> {
                                    logger.debug("Workflow {} finished with status {}, removing it from repository",
                                                 execution.workflowId(),
                                                 finished.state().workflowStatus());
                                    this.workflowExecutionRepository.remove(execution.workflowId());
                                    checkpointWorkIndex.markSafe(execution.workflowId());
                                    checkpointingSupport.requestCheckpoint(replaySupport.currentTrackingToken());
                                }
                        );
                    } catch (Throwable t) {
                        throw new RuntimeException("Error during workflow execution", t);
                    }
                });
    }

    private void checkAndCreateNewWorkflow(@Nonnull EventMessage eventMessage,
                                           @Nonnull ProcessingContext processingContext) {

        var replaySupport = processingContext.component(WorkflowEngineReplaySupport.class);

        // For brand-new starts, only the highest-registered version spawns instances.
        // Older registered versions stay available for replay routing (selected later in the execution path
        // based on state.workflowDefinitionVersion(), itself sourced from the workflow's started event metadata).
        // Same-version duplicates spawn in parallel only if their workflowIdProviders produce distinct ids;
        // otherwise the second spawn is rejected as a same-version duplicate in resolveWorkflowIdForNewSpawn.
        // The "multiple definitions at the same version" warning is emitted ONCE at engine startup (see
        // checkForSameVersionDuplicates) rather than per event.
        var configurations = workflowConfigurationRegistry.getHighestVersionConfigurations(
                eventMessage.type().qualifiedName()
        );
        configurations
                .forEach(configuration -> {

                             if (configuration.predicate().test(eventMessage, processingContext)) {

                                 var workflowConfiguration = configuration.configuration();

                                 var baseWorkflowId = workflowConfiguration.workflowIdProvider().apply(eventMessage);
                                 var workflowId = WorkflowSpawnRouting.resolveWorkflowIdForNewSpawn(
                                         workflowExecutionRepository,
                                         baseWorkflowId,
                                         workflowConfiguration.workflowVersion(),
                                         eventMessage);

                                 if (workflowId == null) {
                                     return;
                                 }

                                 var payload = requireNonNull(
                                         eventMessage.payloadAs(PAYLOAD_TYPE), "Error converting initial payload"
                                 );

                                 var workflowContext = workflowConfiguration
                                         .workflowContextFactory()
                                         .createContext(payload, workflowId, processingContext, workflowConfiguration);

                                 var execution = workflowExecutionRepository.save(workflowId, () -> {
                                     logger.debug("Creating a new workflow '{}' with payload '{}'",
                                                  workflowId, eventMessage.payload());
                                     return workflowConfiguration.workflowExecutionFactory().create(workflowContext);
                                 });
                                 checkpointWorkIndex.register(
                                         execution.workflowId(), execution::registerCheckpointWorkStateListener
                                 );
                                 if (replaySupport.isLiveMode()) {
                                     execute(execution, processingContext);
                                 }
                             }
                         }
                );
    }

    /**
     * Retrieve all workflow executions.
     *
     * @return set of currently running workflow executions.
     */
    @Nonnull
    public Set<WorkflowExecution> workflowExecutions() {
        return workflowExecutionRepository.findAll();
    }

    /**
     * Restores and starts active workflow executions before processor replay resumes.
     * <p>
     * Restoration has two deliberately separate processing contexts. The sourcing context belongs to the startup unit
     * of work and is used only while reading the event-sourced workflow state. This method creates an execution context
     * as a child of the sourcing context; it becomes the parent context of each restored workflow body and is retained
     * after startup for the workflow's lifetime.
     *
     * @param processorToken processor token at startup; initializes replay tracking when no processor token has been
     *                       observed yet
     * @param replayRequired whether the processor must catch up after restored workflows are started
     * @return a future that completes when all running workflows have been restored and the engine is in the required
     * replay or live mode
     */
    public CompletableFuture<Void> start(@Nullable TrackingToken processorToken, boolean replayRequired) {
        return unitOfWorkFactory.create("WorkflowRehydration")
                                .executeWithResult(sourcingContext -> {
                                    var replaySupport = sourcingContext.component(WorkflowEngineReplaySupport.class);
                                    replaySupport.initializeProcessorTokenIfAbsent(processorToken);
                                    var executionUnitOfWork = new SimpleUnitOfWorkFactory(sourcingContext).create();

                                    return executionUnitOfWork.executeWithResult(
                                            executionContext -> loadRunningWorkflows(sourcingContext, executionContext)
                                                    .thenApply(ignored -> {
                                                        removeTerminalAndStartRestoredWorkflowExecutions(
                                                                "before replay catch-up", executionContext
                                                        );
                                                        if (!replayRequired) {
                                                            replaySupport.switchToLiveMode(sourcingContext);
                                                        }
                                                        return null;
                                                    })
                                    );
                                });
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
            execution.interrupt();
        }
        workflowExecutionRepository.clear();
        checkpointWorkIndex.clear();
    }

    private CompletableFuture<Void> loadRunningWorkflows(@Nonnull ProcessingContext sourcingContext,
                                                         @Nonnull ProcessingContext executionContext) {
        var replaySupport = sourcingContext.component(WorkflowEngineReplaySupport.class);

        replaySupport.initializeRestoreProcessingContext(executionContext);
        return workflowStore.loadRunningWorkflows(sourcingContext)
                            .thenCompose(runningWorkflows -> {
                                if (runningWorkflows.workflowIds().isEmpty()) {
                                    logger.debug("No running workflows to rehydrate.");
                                    return CompletableFuture.completedFuture(null);
                                }
                                logger.debug(
                                        "Rehydrating {} running workflow execution(s) from event-sourced state.",
                                        runningWorkflows.workflowIds().size());
                                var rehydrations = runningWorkflows.workflowIds()
                                                                   .stream()
                                                                   .map(workflowId -> workflowStore
                                                                           .loadWorkflow(workflowId,
                                                                                         sourcingContext)
                                                                           .thenAccept(state -> restoreWorkflow(
                                                                                   workflowId,
                                                                                   state,
                                                                                   executionContext
                                                                           )))
                                                                   .toArray(CompletableFuture[]::new);
                                return CompletableFuture.allOf(rehydrations);
                            });
    }

    private void restoreWorkflow(@Nonnull String workflowId,
                                 @Nonnull WorkflowState state,
                                 @Nonnull ProcessingContext executionContext) {
        var workflowConfiguration = workflowConfigurationRegistry
                .getWorkflowConfiguration(state.workflowDefinitionId())
                .orElseThrow(() -> new IllegalStateException(
                        "No workflow configuration found for workflow '%s' with definition %s."
                                .formatted(workflowId, state.workflowDefinitionId())
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
     * Removes terminal workflow executions and starts the remaining restored executions.
     *
     * @param phase startup phase in which the executions are started
     */
    private void removeTerminalAndStartRestoredWorkflowExecutions(
            @Nonnull String phase,
            @Nonnull ProcessingContext processingContext
    ) {
        var terminalExecutions = workflowExecutionRepository.findAll(
                execution -> execution.state().workflowStatus().isTerminal()
        );
        workflowExecutionRepository.removeAll(execution -> execution.state().workflowStatus().isTerminal());
        terminalExecutions.forEach(execution -> checkpointWorkIndex.markSafe(execution.workflowId()));
        var executionsToStart = workflowExecutionRepository.findAll(execution -> !execution.isRunning());
        if (executionsToStart.isEmpty()) {
            logger.info("No restored workflow executions require startup {}.", phase);
            return;
        }
        logger.info("Starting {} restored workflow execution(s) {}.", executionsToStart.size(), phase);
        for (var execution : executionsToStart) {
            execute(execution, processingContext);
        }
        logger.info("Started {} restored workflow execution(s) {}.", executionsToStart.size(), phase);
    }

    @Override
    public boolean hasUnsafeCheckpointWork() {
        return checkpointWorkIndex.hasUnsafeCheckpointWork();
    }

    /**
     * Schedules checkpoint barriers across the unsafe-execution index snapshot.
     * <p>
     * Barriers are added to executions still unsafe when resolved from the index. When the snapshot is empty, the
     * callback re-checks safety in case a workflow appended work concurrently with the snapshot.
     *
     * @param onDrained callback invoked after the snapshot's barriers have been crossed
     */
    @Override
    public void scheduleCheckpointIntent(@Nonnull Runnable onDrained) {
        var executions = new HashSet<WorkflowExecution>();
        for (var workflowId : checkpointWorkIndex.unsafeWorkflowIds()) {
            workflowExecutionRepository.findById(workflowId).ifPresentOrElse(execution -> {
                if (execution.hasUnsafeCheckpointWork()) {
                    executions.add(execution);
                } else {
                    checkpointWorkIndex.markSafe(workflowId);
                }
            }, () -> checkpointWorkIndex.markSafe(workflowId));
        }
        if (executions.isEmpty()) {
            onDrained.run();
            return;
        }
        for (var execution : executions) {
            execution.appendCheckpointIntent(onDrained);
        }
    }

    @Override
    public void onLiveModeActivated(@Nonnull ProcessingContext processingContext) {
        this.workflowConfigurationRegistry.warnAboutSameVersionDuplicates();
        logger.info("Workflow instance replay finished. Switching to live mode.");
        removeTerminalAndStartRestoredWorkflowExecutions("after replay catch-up", processingContext);
    }
}
