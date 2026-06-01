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
import org.axonframework.messaging.eventhandling.processing.streaming.token.WrappedToken;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChangedHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.workflow.runtime.util.ProcessingContextUtils.RESTART_TOKEN_RESOURCE_KEY;

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
public class WorkflowEngine implements EventHandler, ReplayStatusChangedHandler {

    private final Logger logger = LoggerFactory.getLogger(WorkflowEngine.class);

    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private final WorkflowExecutionRepository workflowExecutionRepository;
    private final SafePointStore safePointStore;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicReference<TrackingToken> currentTrackingToken = new AtomicReference<>();
    private final AtomicReference<TrackingToken> lastProcessedTrackingToken = new AtomicReference<>();

    /**
     * Creates a new workflow engine.
     *
     * @param workflowConfigurationRegistry configuration registry.
     * @param workflowExecutionRepository   execution registry.
     * @param safePointStore                engine safe point tracking token store.
     */
    public WorkflowEngine(
            @Nonnull WorkflowConfigurationRegistry<?> workflowConfigurationRegistry,
            @Nonnull WorkflowExecutionRepository workflowExecutionRepository,
            @Nonnull SafePointStore safePointStore
    ) {
        EntitlementManager.INSTANCE.registerAddon(WorkflowAxoniqAddon.class);
        this.workflowConfigurationRegistry = workflowConfigurationRegistry;
        this.workflowExecutionRepository = workflowExecutionRepository;
        this.safePointStore = safePointStore;
    }

    @Nonnull
    @Override
    public MessageStream.Empty<Message> handle(@Nonnull EventMessage eventMessage,
                                               @Nonnull ProcessingContext processingContext) {
        var currentTrackingToken = captureCurrentTrackingToken(processingContext);
        // ensure there is always a restart token resource available, even an empty one
        processingContext.putResource(RESTART_TOKEN_RESOURCE_KEY,
                                      Optional.ofNullable(lastProcessedTrackingToken.get()));
        logger.trace("Received eventMessage {} {} {}",
                     processingContext.resources().get(TrackingToken.RESOURCE_KEY),
                     eventMessage.identifier(),
                     eventMessage.type()
        );
        if (MetadataUtils.hasWorkflowId().test(eventMessage.metadata())) {
            var workflowId = MetadataUtils.getWorkflowId(eventMessage.metadata());
            // TODO: discussion regarding hibernating workflows ->
            // TODO: is it safe to put an eventMessage in the queue?
            var execution = workflowExecutionRepository
                    .findById(workflowId)
                    .orElseThrow(() -> new IllegalStateException("No workflow found for id: " + workflowId));
            execution.onEvent(eventMessage, processingContext);
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

        if (currentTrackingToken != null) {
            lastProcessedTrackingToken.set(currentTrackingToken);
        }
        logger.trace("EventMessage {} successfully handled", eventMessage.identifier());
        return MessageStream.empty();
    }

    /**
     * If the replay is finished, start workflow executions of previously event-sourced executions.
     */
    @Override
    @Nonnull
    public MessageStream.Empty<Message> handle(@Nonnull ReplayStatusChanged statusChange,
                                               @Nonnull ProcessingContext context) {
        captureCurrentTrackingToken(context);
        logger.debug("Replay status changed to {} at {}",
                     statusChange.status(),
                     context.resources().get(TrackingToken.RESOURCE_KEY));

        if (!statusChange.status().isReplay()) {
            switchToLiveMode();
        }
        return MessageStream.empty();
    }


    /**
     * Switches the engine to live mode. By doing so, the engine stops replaying events and starts executing workflow
     * executions. Prior to that, all finished workflow executions are removed from the execution repository.
     */
    public void switchToLiveMode() {
        var running = isRunning.getAndSet(true);
        if (!running) {
            logger.info("Workflow instance replay finished. Switching to live mode.");
            // get rid of finished executions
            workflowExecutionRepository
                    .findAll()
                    .stream()
                    .filter(e -> e.state().workflowStatus().isTerminal())
                    .map(WorkflowExecution::workflowId)
                    .forEach(workflowExecutionRepository::remove);
            persistEngineSafePoint();

            var allExecution = workflowExecutionRepository.findAll();
            if (allExecution.isEmpty()) {
                logger.info("No running workflow instances found.");
            } else {
                logger.info("Restored {} running workflow instances, starting workflow execution.",
                            allExecution.size());
                for (var execution : allExecution) {
                    execute(execution);
                }
                logger.info("All workflow instances started.");
            }
        } else {
            logger.warn("Workflow Execution is already started.");
        }
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
                                    persistEngineSafePoint();
                                }
                        );
                    } catch (Throwable t) {
                        throw new RuntimeException("Error during workflow execution", t);
                    }
                });
    }

    private void checkAndCreateNewWorkflow(@Nonnull EventMessage eventMessage,
                                           @Nonnull ProcessingContext processingContext) {
        var configurations = workflowConfigurationRegistry.getWorkflowsConfigurations(
                eventMessage.type().qualifiedName()
        );
        configurations
                .forEach(configuration -> {

                             if (configuration.predicate().test(eventMessage, processingContext)) {

                                 var workflowConfiguration = configuration.configuration();

                                 var workflowId = workflowConfiguration.workflowIdProvider().apply(eventMessage);

                                 if (workflowExecutionRepository.findById(workflowId).isPresent()) {
                                     logger.warn(
                                             "A workflow with id '{}' is already running; ignoring new start request triggered by event '{}'. "
                                                     + "If this was intentional, associate each parallel workflow with a different idProperty so every instance gets a unique id.",
                                             workflowId,
                                             eventMessage.type().qualifiedName()
                                     );
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
                                     logger.debug("Creating a new workflow with '{}'", eventMessage.payload());
                                     return workflowConfiguration.workflowExecutionFactory().create(workflowContext);
                                 });
                                 persistEngineSafePoint();
                                 if (isRunning.get()) { // if the engine is already running, start the workflow immediately
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
        persistEngineSafePoint();
        workflowExecutionRepository.clear();
    }

    /**
     * Initializes the current and last processed tracking tokens.
     *
     * @param safePoint safe point tracking token passed on reset / empty store start.
     */
    public void initializeSafePoint(@Nullable TrackingToken safePoint) {
        lastProcessedTrackingToken.set(safePoint);
        currentTrackingToken.set(safePoint);
    }

    @Nullable
    private TrackingToken captureCurrentTrackingToken(@Nonnull ProcessingContext processingContext) {
        var token = (TrackingToken) processingContext.resources().get(TrackingToken.RESOURCE_KEY);
        if (token != null) {
            // Unwrap ReplayToken (and any other WrappedToken) to the raw underlying position before storing.
            // Without this, restart tokens captured across replay events mix raw and wrapped types, which
            // makes determineEngineSafePoint crash with "Incompatible token type provided: ReplayToken".
            currentTrackingToken.set(WrappedToken.unwrapLowerBound(token));
        }
        return currentTrackingToken.get();
    }

    private void persistEngineSafePoint() {
        var safePoint = determineEngineSafePoint();
        if (safePoint != null) {
            safePointStore.storeSafePointToken(safePoint).join();
        }
    }

    private TrackingToken determineEngineSafePoint() {
        var executions = workflowExecutionRepository.findAll();
        if (executions.isEmpty()) {
            return currentTrackingToken.get();
        }
        TrackingToken earliestToken = null;
        for (var execution : executions) {
            var executionToken = execution.restartToken();
            if (executionToken == null) {
                return null;
            }
            earliestToken = earliestToken == null ? executionToken : earliestToken.lowerBound(executionToken);
        }
        return earliestToken;
    }
}
