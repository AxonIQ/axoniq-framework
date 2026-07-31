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

import io.axoniq.framework.messaging.eventstreaming.checkpoint.Checkpointing;
import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.workflow.runtime.execution.EventSourcedWorkflowState.PAYLOAD_TYPE;

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
public class WorkflowEngine implements EventHandler, CheckpointingSupplier, ReplayStatusChangedHandlerSupplier {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngine.class);

    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private final WorkflowExecutionRepository workflowExecutionRepository;
    private final WorkflowCancellationService workflowCancellationService;
    private final WorkflowStore workflowStore;
    private final WorkflowEngineReplaySupport replaySupport;
    private final WorkflowEngineCheckpointingAdvancingSupport checkpointingSupport;

    /**
     * Creates a new workflow engine.
     *
     * @param workflowConfigurationRegistry configuration registry.
     * @param workflowExecutionRepository   execution registry.
     * @param workflowCancellationService   workflow cancellation service.
     * @param workflowStore                 store for persistence matters.
     */
    public WorkflowEngine(
            @Nonnull WorkflowConfigurationRegistry<?> workflowConfigurationRegistry,
            @Nonnull WorkflowExecutionRepository workflowExecutionRepository,
            @Nonnull WorkflowCancellationService workflowCancellationService,
            @Nonnull WorkflowStore workflowStore
    ) {
        EntitlementManager.INSTANCE.registerAddon(WorkflowAxoniqAddon.class);
        this.workflowConfigurationRegistry = workflowConfigurationRegistry;
        this.workflowExecutionRepository = workflowExecutionRepository;
        this.workflowCancellationService = workflowCancellationService;
        this.workflowStore = workflowStore;
        this.replaySupport = new WorkflowEngineReplaySupport(
                () -> {
                    workflowConfigurationRegistry.warnAboutSameVersionDuplicates();
                    logger.info("Workflow instance replay finished. Switching to live mode.");
                    removeTerminalAndStartRestoredWorkflowExecutions("after replay catch-up");
                }
        );
        this.checkpointingSupport = new WorkflowEngineCheckpointingAdvancingSupport(
                this::scheduleCheckpointIntent
        );
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
                replaySupport.advanceReplayPosition(currentTrackingToken);
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
        replaySupport.advanceReplayPosition(currentTrackingToken);
        logger.trace("EventMessage {} successfully handled", eventMessage.identifier());
        return MessageStream.empty();
    }

    void requestCheckpoint(@Nullable TrackingToken token) {
        checkpointingSupport.requestCheckpoint(token);
    }

    private void execute(@Nonnull WorkflowExecution execution) {
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
                                    removeExecution(execution.workflowId());
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

                                 var payload = Objects.requireNonNull(eventMessage.payloadAs(PAYLOAD_TYPE),
                                                                      "Error converting initial payload");

                                 var workflowContext = workflowConfiguration
                                         .workflowContextFactory()
                                         .createContext(payload, workflowId, processingContext, workflowConfiguration);

                                 var execution = workflowExecutionRepository.save(workflowId, () -> {
                                     logger.debug("Creating a new workflow '{}' with payload '{}'",
                                                  workflowId, eventMessage.payload());
                                     return workflowConfiguration.workflowExecutionFactory().create(workflowContext);
                                 });
                                 registerCancellation(execution);
                                 if (replaySupport.isLiveMode()) {
                                     execute(execution);
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
     * Returns the checkpointing aspect used by Axon's event processor.
     *
     * @return checkpointing support
     */
    @Nonnull
    @Override
    public Checkpointing checkpointing() {
        return checkpointingSupport;
    }

    /**
     * Returns the replay-status handling aspect used by Axon's event processor and workflow bootstrap.
     *
     * @return replay-status handling support
     */
    @Nonnull
    @Override
    public WorkflowEngineReplaySupport replayStatusChangedHandler() {
        return replaySupport;
    }

    /**
     * Restores and starts active workflow executions before processor replay resumes.
     * <p>
     * Restoration has two deliberately separate processing contexts. The sourcing context belongs to the startup unit
     * of work and is used only while reading the event-sourced workflow state. The execution context becomes the parent
     * context of each restored workflow body and is retained after startup for the workflow's lifetime.
     *
     * @param processorToken   processor token at startup; initializes replay tracking when no processor token has been
     *                         observed yet
     * @param sourcingContext  context of the short-lived startup unit of work used to load durable workflow state; it
     *                         may carry event-store transactions and lifecycle handlers and must not be retained by a
     *                         restored workflow
     * @param executionContext independent context used as the parent of restored workflow executions; it provides the
     *                         same application components while keeping workflow-body resources and lifecycle work
     *                         separate from startup
     *                         <p>
     *                         Reusing {@code sourcingContext} here is invalid because restored workflow bodies run
     *                         asynchronously and can outlive startup. If such a body appends an event after the startup
     *                         unit of work has entered {@code COMMIT}, Axon can no longer register the required
     *                         {@code PREPARE_COMMIT} handler. The append operation then fails, and the workflow cannot
     *                         persist its resumed, timed-out, or terminal state
     */
    public void start(@Nullable TrackingToken processorToken,
                      @Nonnull ProcessingContext sourcingContext,
                      @Nonnull ProcessingContext executionContext) {
        replaySupport.initializeProcessorTokenIfAbsent(processorToken);
        loadRunningWorkflows(sourcingContext, executionContext);
        removeTerminalAndStartRestoredWorkflowExecutions("before replay catch-up");
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
        workflowCancellationService.clear();
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

    private void loadRunningWorkflows(@Nonnull ProcessingContext sourcingContext,
                                      @Nonnull ProcessingContext executionContext) {
        replaySupport.initializeRestoreProcessingContext(executionContext);
        var runningWorkflows = workflowStore.loadRunningWorkflows(sourcingContext).join();
        if (runningWorkflows.workflowIds().isEmpty()) {
            logger.info("No running workflows to rehydrate.");
            return;
        }
        logger.info("Loading {} running workflow execution(s) from event-sourced state.",
                    runningWorkflows.workflowIds().size());
        for (var workflowId : runningWorkflows.workflowIds()) {
            var state = workflowStore.loadWorkflow(workflowId, sourcingContext).join();
            var workflowDefinitionId = state.workflowDefinitionId();
            var workflowConfiguration = workflowConfigurationRegistry
                    .getWorkflowConfiguration(workflowDefinitionId)
                    .orElseThrow(() -> new IllegalStateException(
                            "No workflow configuration found for workflow '%s' with definition %s."
                                    .formatted(workflowId, workflowDefinitionId)
                    ));
            var workflowContext = workflowConfiguration.workflowContextFactory().createContext(
                    state.payload(),
                    workflowId,
                    executionContext,
                    workflowConfiguration
            );
            var execution = workflowConfiguration.workflowExecutionFactory().create(workflowContext);
            execution.initializeState(state);
            workflowExecutionRepository.save(workflowId, () -> execution);
        }
    }

    /**
     * Removes terminal workflow executions and starts the remaining restored executions.
     *
     * @param phase startup phase in which the executions are started
     */
    private void removeTerminalAndStartRestoredWorkflowExecutions(@Nonnull String phase) {
        workflowExecutionRepository.removeAll(execution -> execution.state().workflowStatus().isTerminal());
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

    private boolean scheduleCheckpointIntent(@NonNull Runnable onDrained) {
        var scheduledWorkflowIds = new HashSet<String>();
        var pendingExecutions = workflowExecutionRepository.findAll(WorkflowExecution::hasPendingCheckpointWork)
                                                           .stream()
                                                           .filter(execution -> scheduledWorkflowIds.add(execution.workflowId()))
                                                           .toList();
        if (pendingExecutions.isEmpty()) {
            return false;
        }
        var remainingBarriers = new AtomicInteger(pendingExecutions.size());
        for (var execution : pendingExecutions) {
            execution.appendCheckpointIntent(() -> {
                if (remainingBarriers.decrementAndGet() == 0) {
                    onDrained.run();
                }
            });
        }
        return true;
    }
}
