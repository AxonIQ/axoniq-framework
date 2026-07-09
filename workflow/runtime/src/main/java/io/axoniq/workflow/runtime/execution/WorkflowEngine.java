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
import io.axoniq.framework.messaging.eventstreaming.checkpoint.Checkpointing;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowDefinitionId;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
    private final WorkflowStateRehydrationSupport workflowStateRehydrationSupport;
    private final WorkflowEngineReplaySupport replaySupport;
    private final WorkflowEngineCheckpointingAdvancingSupport checkpointingSupport;

    /**
     * Creates a new workflow engine.
     *
     * @param workflowConfigurationRegistry   configuration registry.
     * @param workflowExecutionRepository     execution registry.
     * @param workflowStateRehydrationSupport repository-backed rehydration support.
     */
    public WorkflowEngine(
            @Nonnull WorkflowConfigurationRegistry<?> workflowConfigurationRegistry,
            @Nonnull WorkflowExecutionRepository workflowExecutionRepository,
            @Nonnull WorkflowStateRehydrationSupport workflowStateRehydrationSupport
    ) {
        EntitlementManager.INSTANCE.registerAddon(WorkflowAxoniqAddon.class);
        this.workflowConfigurationRegistry = workflowConfigurationRegistry;
        this.workflowExecutionRepository = workflowExecutionRepository;
        this.workflowStateRehydrationSupport = workflowStateRehydrationSupport;
        this.replaySupport = new WorkflowEngineReplaySupport(this::onLiveModeActivated);
        this.checkpointingSupport = new WorkflowEngineCheckpointingAdvancingSupport(new WorkflowEngineCheckpointingAdvancingSupport.Host() {
            @Override
            public boolean hasUnsafeCheckpointWork() {
                return WorkflowEngine.this.hasUnsafeCheckpointWork();
            }

            @Override
            public boolean scheduleCheckpointIntent(@Nonnull Runnable onDrained) {
                return WorkflowEngine.this.scheduleCheckpointIntent(onDrained);
            }
        });
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
                // TODO: discussion regarding hibernating workflows ->
                // TODO: is it safe to put an eventMessage in the queue?
                execution.onEvent(eventMessage, processingContext);
            }
        }

        checkpointingSupport.requestCheckpoint(currentTrackingToken);
        replaySupport.advanceReplayPosition(currentTrackingToken);
        logger.trace("EventMessage {} successfully handled", eventMessage.identifier());
        return MessageStream.empty();
    }

    /**
     * Switches the engine to live mode. By doing so, the engine stops replaying events and starts executing workflow
     * executions. Prior to that, all finished workflow executions are removed from the execution repository.
     */
    public void switchToLiveMode() {
        if (!replaySupport.switchToLiveMode()) {
            logger.warn("Workflow execution is already started.");
        }
    }

    /**
     * Loads running workflow ids and rehydrates fresh workflow executions from event-sourced workflow state.
     *
     * @param sourcingContext processing context used to load state and create restored executions
     * @param executionContext processing context used to execute
     */
    public void rehydrateRunningWorkflows(@Nonnull ProcessingContext sourcingContext,
                                          @Nonnull ProcessingContext executionContext) {
        replaySupport.initializeRestoreProcessingContext(executionContext);
        var runningWorkflows = workflowStateRehydrationSupport.loadRunningWorkflows(sourcingContext);
        if (runningWorkflows.workflowIds().isEmpty()) {
            logger.info("No running workflows to rehydrate.");
            return;
        }
        logger.info("Rehydrating {} running workflow execution(s) from event-sourced state.",
                    runningWorkflows.workflowIds().size());
        for (var workflowId : runningWorkflows.workflowIds()) {
            var state = workflowStateRehydrationSupport.loadWorkflowState(workflowId, sourcingContext);
            var workflowConfiguration = resolveWorkflowConfiguration(workflowId, state);
            var workflowContext = workflowConfiguration.workflowContextFactory().createContext(
                    state.payload(),
                    workflowId,
                    executionContext,
                    workflowConfiguration
            );
            var execution = workflowConfiguration.workflowExecutionFactory().create(workflowContext);
            if (!(execution instanceof WorkflowStateRehydratable restorable)) {
                throw new IllegalStateException(
                        "Workflow execution for workflowId '%s' does not support state rehydration.".formatted(
                                workflowId)
                );
            }
            restorable.rehydrate(state);
            workflowExecutionRepository.save(workflowId, () -> execution);
        }
    }

    /**
     * Starts restored workflow executions before processor replay resumes so transient wait registrations are rebuilt.
     */
    public void startCheckpointCatchUp() {
        var executionsToStart = workflowExecutionRepository
                .findAll()
                .stream()
                .filter(execution -> !execution.isExecutable())
                .filter(execution -> !execution.state().workflowStatus().isTerminal())
                .toList();
        if (executionsToStart.isEmpty()) {
            return;
        }
        logger.info("Starting {} rehydrated workflow execution(s) before replay catch-up.", executionsToStart.size());
        for (var execution : executionsToStart) {
            execute(execution);
        }
    }

    /**
     * Retrieve all workflow executions.
     *
     * @return set of currently running workflow executions
     */
    public Set<WorkflowExecution> workflowExecutions() {
        return workflowExecutionRepository.findAll();
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
    }

    void requestCheckpoint(@Nullable TrackingToken token) {
        checkpointingSupport.requestCheckpoint(token);
    }

    private void onLiveModeActivated() {
        workflowConfigurationRegistry.warnAboutSameVersionDuplicates();
        logger.info("Workflow instance replay finished. Switching to live mode.");
        workflowExecutionRepository
                .findAll()
                .stream()
                .filter(e -> e.state().workflowStatus().isTerminal())
                .map(WorkflowExecution::workflowId)
                .forEach(workflowExecutionRepository::remove);

        var allExecution = workflowExecutionRepository.findAll();
        if (allExecution.isEmpty()) {
            logger.info("No running workflow instances found.");
        } else {
            var executionsToStart = allExecution.stream()
                                                .filter(execution -> !execution.isExecutable())
                                                .toList();
            logger.info("Restored {} running workflow instances, starting {} workflow execution(s).",
                        allExecution.size(),
                        executionsToStart.size());
            for (var execution : executionsToStart) {
                execute(execution);
            }
            logger.info("All workflow instances started.");
        }
    }

    private boolean scheduleCheckpointIntent(@Nonnull Runnable onDrained) {
        var scheduledWorkflowIds = new HashSet<String>();
        var scheduled = false;
        for (var execution : workflowExecutionRepository.findAll()) {
            if (!execution.hasPendingCheckpointWork() || !scheduledWorkflowIds.add(execution.workflowId())) {
                continue;
            }
            execution.appendCheckpointIntent(onDrained);
            scheduled = true;
        }
        return scheduled;
    }

    private boolean hasUnsafeCheckpointWork() {
        return workflowExecutionRepository
                .findAll().stream()
                .anyMatch(WorkflowExecution::hasPendingCheckpointWork);
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
                                    this.workflowExecutionRepository.remove(execution.workflowId());
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

                                 var payload = Objects.requireNonNull(eventMessage.payloadAs(
                                         new TypeReference<Map<String, Object>>() {
                                         }
                                 ), "Error converting initial payload");

                                 var workflowContext = workflowConfiguration
                                         .workflowContextFactory()
                                         .createContext(payload, workflowId, processingContext, workflowConfiguration);

                                 var execution = workflowExecutionRepository.save(workflowId, () -> {
                                     logger.debug("Creating a new workflow '{}' with payload '{}'",
                                                  workflowId, eventMessage.payload());
                                     return workflowConfiguration.workflowExecutionFactory().create(workflowContext);
                                 });
                                 if (replaySupport.isLiveMode()) {
                                     execute(execution);
                                 }
                             }
                         }
                );
    }

    private WorkflowConfiguration<?> resolveWorkflowConfiguration(@Nonnull String workflowId,
                                                                  @Nonnull EventSourcedWorkflowState state) {
        WorkflowDefinitionId workflowDefinitionId = state.workflowDefinitionId();
        return workflowConfigurationRegistry.getWorkflowConfiguration(workflowDefinitionId)
                                            .orElseThrow(() -> new IllegalStateException(
                                                    "No workflow configuration found for workflow '%s' with definition %s."
                                                            .formatted(workflowId, workflowDefinitionId)
                                            ));
    }
}
