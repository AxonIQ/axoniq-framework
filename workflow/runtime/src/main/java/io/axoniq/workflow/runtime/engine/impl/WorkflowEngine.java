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
package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChangedHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.engine.configuration.WorkflowEnhancer.WORKFLOW_ENGINE_EXECUTOR;

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

    public WorkflowEngine(
            @Nonnull WorkflowConfigurationRegistry<?> workflowConfigurationRegistry,
            @Nonnull WorkflowExecutionRepository workflowExecutionRepository
    ) {
        this.workflowConfigurationRegistry = workflowConfigurationRegistry;
        this.workflowExecutionRepository = workflowExecutionRepository;
    }

    @NotNull
    @Override
    public MessageStream.Empty<Message> handle(@NotNull EventMessage eventMessage,
                                               @NotNull ProcessingContext processingContext) {
        logger.trace("Received eventMessage {} {} by thread {}",
                     eventMessage.identifier(),
                     eventMessage.type(),
                     Thread.currentThread());
        if (MetadataUtils.hasWorkflowId().test(eventMessage.metadata())) {
            var workflowId = MetadataUtils.getWorkflowId(eventMessage.metadata());
            // TODO: discussion regarding hibernating workflows ->
            // TODO: is it safe to put an eventMessage in the queue?
            var execution = workflowExecutionRepository.findById(workflowId)
                                                       .orElseThrow(() -> new IllegalStateException(
                                                               "No workflow found for id: " + workflowId));
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
     * This is a place to be called from Event Processor
     */
    public void runWorkflows() {
        var running = isRunning.getAndSet(true);
        if (!running) {
            logger.info("Starting Workflow Execution.");
            for (var execution : workflowExecutionRepository.findAll()) {
                executeInstance(execution);
            }
            logger.info("Starting completed.");
        } else {
            logger.warn("Workflow Execution is already started.");
        }
    }

    void executeInstance(@Nonnull WorkflowExecution execution) {

        boolean runInNewThread = true; // FIXME -> this is the only way to run it currently.
        if (runInNewThread) {
            execution.workflowContext().processingContext().component(Executor.class, WORKFLOW_ENGINE_EXECUTOR).execute(
                    () -> {
                        this.execute(execution).accept(execution.workflowContext().processingContext());
                    });
        } else {
            execution.workflowContext()
                     .processingContext()
                     .whenComplete(this.execute(execution));
        }
    }

    private Consumer<ProcessingContext> execute(@Nonnull WorkflowExecution execution) {
        return pc -> {
            try {
                logger.info("Executing workflow {}", execution.workflowId());
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
        };
    }


    /**
     * If the replay is finished, start workflow executions.
     */
    @Override
    @Nonnull
    public MessageStream.Empty<Message> handle(@Nonnull ReplayStatusChanged statusChange,
                                               @Nonnull ProcessingContext context) {
        if (!statusChange.status().isReplay()) {
            runWorkflows();
        }
        return MessageStream.empty();
    }

    private void checkAndCreateNewWorkflow(@Nonnull EventMessage eventMessage,
                                           @Nonnull ProcessingContext processingContext) {
        var definitions = workflowConfigurationRegistry.getWorkflowsConfigurations(eventMessage.type().qualifiedName());
        definitions.forEach(predicatedWorkflowConfiguration -> {

                                if (predicatedWorkflowConfiguration.predicate().test(eventMessage)) {

                                    var workflowConfiguration = predicatedWorkflowConfiguration.configuration();

                                    var payload = Objects.requireNonNull(eventMessage.payloadAs(
                                            new TypeReference<Map<String, Object>>() {
                                            },
                                            processingContext.component(Converter.class)
                                    ), "Error converting initial payload");
                                    var workflowId = workflowConfiguration.workflowIdProvider().apply(eventMessage);

                                    var workflowContext = workflowConfiguration
                                            .workflowContextFactory()
                                            .createContext(payload,
                                                           workflowId,
                                                           processingContext,
                                                           workflowConfiguration);

                                    // avoid multiple workflows for the same workflow id.
                                    var execution = workflowExecutionRepository.save(workflowId, () -> {
                                        logger.info("Starting new workflow with '{}'", eventMessage.payload());
                                        return workflowConfiguration.workflowExecutionFactory().create(workflowContext);
                                    });
                                    if (isRunning.get()) {
                                        executeInstance(execution);
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
