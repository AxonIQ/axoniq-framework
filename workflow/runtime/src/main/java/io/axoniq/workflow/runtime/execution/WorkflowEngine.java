/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChangedHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    /**
     * Creates a new workflow engine.
     *
     * @param workflowConfigurationRegistry configuration registry.
     * @param workflowExecutionRepository   execution registry.
     */
    public WorkflowEngine(
            @Nonnull WorkflowConfigurationRegistry<?> workflowConfigurationRegistry,
            @Nonnull WorkflowExecutionRepository workflowExecutionRepository
    ) {
        this.workflowConfigurationRegistry = workflowConfigurationRegistry;
        this.workflowExecutionRepository = workflowExecutionRepository;
    }

    @Nonnull
    @Override
    public MessageStream.Empty<Message> handle(@Nonnull EventMessage eventMessage,
                                               @Nonnull ProcessingContext processingContext) {
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
        logger.debug("Replay status changed to {} at {}",
                    statusChange.status(),
                    context.resources().get(TrackingToken.RESOURCE_KEY));

        if (!statusChange.status().isReplay()) {
            executeEventSourcedWorkflows();
        }
        return MessageStream.empty();
    }


    private void executeEventSourcedWorkflows() {
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

                             if (configuration.predicate().test(eventMessage)) {

                                 var workflowConfiguration = configuration.configuration();

                                 var workflowId = workflowConfiguration.workflowIdProvider().apply(eventMessage);

                                 if (workflowExecutionRepository.findById(workflowId).isPresent()) {
                                     logger.warn(
                                             "A workflow with id '{}' is already running; ignoring new start request triggered by event '{}'. "
                                                     + "If this was intentional, associate each parallel workflow with a different idProperty so every instance gets a unique id.",
                                             workflowId, eventMessage.type().qualifiedName()
                                     );
                                     return;
                                 }

                                 var payload = Objects.requireNonNull(eventMessage.payloadAs(
                                         new TypeReference<Map<String, Object>>() {
                                         },
                                         processingContext.component(Converter.class)
                                 ), "Error converting initial payload");

                                 var workflowContext = workflowConfiguration
                                         .workflowContextFactory()
                                         .createContext(payload, workflowId, processingContext, workflowConfiguration);

                                 var execution = workflowExecutionRepository.save(workflowId, () -> {
                                     logger.debug("Creating a new workflow with '{}'", eventMessage.payload());
                                     return workflowConfiguration.workflowExecutionFactory().create(workflowContext);
                                 });
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
     */
    public void shutdown() {
        workflowExecutionRepository.clear();
    }
}
