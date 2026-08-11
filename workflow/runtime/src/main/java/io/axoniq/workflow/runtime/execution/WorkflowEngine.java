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
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.NonNull;
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
        WorkflowEngineReplaySupport.LiveModeActivatedCallback,
        WorkflowEngineCheckpointingSupport.CheckpointLatchCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngine.class);

    private static final String AFTER_REPLAY_LOG = "after replay catch-up";
    private static final String BEFORE_REPLAY_LOG = "before replay catch-up";

    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private final WorkflowExecutionRepository workflowExecutionRepository;
    private final WorkflowCancellationService workflowCancellationService;
    private final WorkflowStore workflowStore;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private WorkflowEngineReplaySupport replaySupport;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;
    private final UnsafeCheckpointWorkIndex checkpointWorkIndex = new UnsafeCheckpointWorkIndex();

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
    public WorkflowEngine(
            @Nonnull WorkflowConfigurationRegistry<?> workflowConfigurationRegistry,
            @Nonnull WorkflowExecutionRepository workflowExecutionRepository,
            @Nonnull WorkflowCancellationService workflowCancellationService,
            @Nonnull WorkflowStore workflowStore,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory
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
        EntitlementManager.INSTANCE.registerAddon(WorkflowAxoniqAddon.class);
    }

    /**
     * Sets {@code WorkflowEngine} support components which are <b>required</b> for the engine to work.
     * <p>
     * Both {@code replaySupport} and {@code checkpointingSupport} are set outside the
     * {@link #WorkflowEngine(WorkflowConfigurationRegistry, WorkflowExecutionRepository, WorkflowStore,
     * UnitOfWorkFactory)}, because they require <b>this</b> {@code WorkflowEngine} itself to function. Hence, a cyclic
     * dependency would exist upon start-up if done otherwise.
     *
     * @param replaySupport        provides replayability support to this {@code WorkflowEngine}
     * @param checkpointingSupport provides checkpointing support to this {@code WorkflowEngine}
     */
    @Internal
    public void setEngineSupportComponents(@Nonnull WorkflowEngineReplaySupport replaySupport,
                                           @Nonnull WorkflowEngineCheckpointingSupport checkpointingSupport) {
        this.replaySupport = requireNonNull(replaySupport, "The WorkflowEngineReplaySupport must not be null.");
        this.checkpointingSupport = requireNonNull(
                checkpointingSupport, "The WorkflowEngineCheckpointingSupport must not be null."
        );
    }

    @Nonnull
    @Override
    public MessageStream.Empty<Message> handle(@Nonnull EventMessage event,
                                               @Nonnull ProcessingContext context) {
        TrackingToken currentToken = replaySupport.getAndSetTokenFrom(context);
        logger.trace("Handling event [{}] with id [{}] and token [{}].",
                     event.type(), event.identifier(), currentToken);
        checkpointingSupport.getAndSetTriggerFrom(context);

        if (MetadataUtils.hasWorkflowId().test(event.metadata())) {
            var workflowId = MetadataUtils.getWorkflowId(event.metadata());
            // Skip events whose workflowId isn't owned by this engine (expected in multi-module setups).
            var executionOpt = workflowExecutionRepository.findById(workflowId);
            if (executionOpt.isEmpty()) {
                logger.debug("Ignoring event [{}] for workflowId [{}] — no matching execution in this engine.",
                             event.type(), workflowId);
                checkpointingSupport.requestCheckpoint(currentToken);
                replaySupport.validateIfReplayFinished(currentToken, context);
                return MessageStream.empty();
            }
            executionOpt.get().onEvent(event, context);
        } else {
            // handle starting of new processes
            checkAndCreateNewWorkflow(event, context);
            // route external events to workflows waiting for them
            for (var execution : workflowExecutionRepository.findAll()) {
                execution.onEvent(event, context);
            }
        }

        checkpointingSupport.requestCheckpoint(currentToken);
        replaySupport.validateIfReplayFinished(currentToken, context);
        logger.trace("Event [{}] handled successfully.", event.identifier());
        return MessageStream.empty();
    }

    private void checkAndCreateNewWorkflow(@Nonnull EventMessage eventMessage,
                                           @Nonnull ProcessingContext processingContext) {
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

        configurations.forEach(configuration -> {
            if (!configuration.predicate().test(eventMessage, processingContext)) {
                return;
            }

            var workflowConfiguration = configuration.configuration();
            var baseWorkflowId = workflowConfiguration.workflowIdProvider().apply(eventMessage);
            var workflowId = WorkflowSpawnRouting.resolveWorkflowIdForNewSpawn(
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
            if (replaySupport.inLiveMode()) {
                execute(execution);
            }
        });
    }

    @Override
    public void invoke(@Nonnull ProcessingContext context) {
        this.workflowConfigurationRegistry.warnAboutSameVersionDuplicates();
        logger.info("Workflow instance replay finished. Switching to live mode.");
        removeTerminalAndStartRestoredWorkflowExecutions(AFTER_REPLAY_LOG);
    }

    @Override
    public boolean hasUnsafeCheckpointWork() {
        return checkpointWorkIndex.hasUnsafeCheckpointWork();
    }

    /**
     * Adds a checkpoint latch across the current set of {@link WorkflowExecution WorkflowExecutions} that are still
     * performing tasks.
     * <p>
     * The given {@code latch} is attached to all {@code WorkflowExecutions} that still have tasks to perform. Or in
     * other terms, executions that are "unsafe" to checkpoint on
     *
     * @param latch the latch to invoke after all unsafe {@link WorkflowExecution WorkflowExecutions} have reached it
     */
    @Override
    public void addCheckpointLatch(@Nonnull Runnable latch) {
        Set<WorkflowExecution> executions = new HashSet<>();
        for (String workflowId : checkpointWorkIndex.unsafeWorkflowIds()) {
            workflowExecutionRepository.findById(workflowId).ifPresentOrElse(
                    execution -> {
                        if (execution.hasUnsafeCheckpointWork()) {
                            executions.add(execution);
                        } else {
                            checkpointWorkIndex.markSafe(workflowId);
                        }
                    },
                    () -> checkpointWorkIndex.markSafe(workflowId));
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
        return unitOfWorkFactory.create("WorkflowRehydration").executeWithResult(
                sourcingContext -> {
                    replaySupport.setCurrentTokenIfNull(processorToken);
                    UnitOfWork executionUnitOfWork = new SimpleUnitOfWorkFactory(sourcingContext).create();

                    return executionUnitOfWork.executeWithResult(
                            executionContext -> loadRunningWorkflows(
                                    sourcingContext, putCurrentTokenOn(executionContext)
                            ).thenApply(ignored -> {
                                removeTerminalAndStartRestoredWorkflowExecutions(BEFORE_REPLAY_LOG);
                                if (!replayRequired) {
                                    replaySupport.switchToLiveMode(sourcingContext);
                                }
                                return null;
                            })
                    );
                }
        );
    }

    private CompletableFuture<Void> loadRunningWorkflows(@Nonnull ProcessingContext sourcingContext,
                                                         @Nonnull ProcessingContext executionContext) {
        return workflowStore.loadRunningWorkflows(sourcingContext)
                            .thenCompose(runningWorkflows -> {
                                if (runningWorkflows.workflowIds().isEmpty()) {
                                    logger.debug("No running workflows to rehydrate.");
                                    return CompletableFuture.completedFuture(null);
                                }
                                logger.debug(
                                        "Rehydrating {} running workflow execution(s) from event-sourced state.",
                                        runningWorkflows.workflowIds().size()
                                );

                                var rehydrations = runningWorkflows.workflowIds().stream().map(
                                        workflowId -> workflowStore.loadWorkflow(workflowId, sourcingContext)
                                                                   .thenAccept(state -> restoreWorkflow(
                                                                           workflowId,
                                                                           state,
                                                                           executionContext
                                                                   ))
                                ).toArray(CompletableFuture[]::new);
                                return CompletableFuture.allOf(rehydrations);
                            });
    }

    private ProcessingContext putCurrentTokenOn(@NonNull ProcessingContext context) {
        TrackingToken processorToken = replaySupport.currentToken();
        if (processorToken != null && !context.resources().containsKey(TrackingToken.RESOURCE_KEY)) {
            context.putResource(TrackingToken.RESOURCE_KEY, processorToken);
        }
        return context;
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
    private void removeTerminalAndStartRestoredWorkflowExecutions(@Nonnull String phase) {
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
            registerCancellation(execution);
            execute(execution);
        }
        logger.info("Started {} restored workflow execution(s) {}.", executionsToStart.size(), phase);
    }

    private void execute(@Nonnull WorkflowExecution execution) {
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
                                     checkpointingSupport.requestCheckpoint(replaySupport.currentToken());
                                 }
                         );
                     } catch (Throwable t) {
                         throw new RuntimeException("Error during workflow execution", t);
                     }
                 });
    }

    private void registerCancellation(@Nonnull WorkflowExecution execution) {
        if (execution instanceof WorkflowCancellationProvider provider) {
            workflowCancellationService.register(execution.workflowId(), provider.workflowCancellation());
        }
    }

    private void removeExecution(@Nonnull String workflowId) {
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
}
