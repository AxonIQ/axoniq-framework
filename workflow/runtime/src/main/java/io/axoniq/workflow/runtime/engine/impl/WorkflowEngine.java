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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.WorkflowDefinitionRegistry;
import io.axoniq.workflow.runtime.api.WorkflowExecution;
import io.axoniq.workflow.runtime.api.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class WorkflowEngine implements EventHandler {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final WorkflowDefinitionRegistry<?> workflowDefinitionRegistry;
    private final WorkflowExecutionRepository workflowRepository;

    public WorkflowEngine(
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull EventSink eventSink,
            @Nonnull WorkflowDefinitionRegistry<?> workflowDefinitionRegistry,
            @Nonnull WorkflowExecutionRepository workflowRepository
    ) {
        this.workflowDefinitionRegistry = workflowDefinitionRegistry;
        this.workflowRepository = workflowRepository;
    }

    @NotNull
    @Override
    public MessageStream.Empty<Message> handle(@NotNull EventMessage eventMessage,
                                               @NotNull ProcessingContext processingContext) {
        logger.trace("Received eventMessage {}", eventMessage.type());
        if (MetadataUtils.hasWorkflowId().test(eventMessage.metadata())) {
            var workflowId = MetadataUtils.getWorkflowId(eventMessage.metadata());
            // TODO: discussion regarding hibernating workflows ->
            // TODO: is it safe to put an eventMessage in the queue?
            workflowRepository.findById(workflowId)
                              .orElseThrow(() -> new IllegalStateException("No workflow found for id: " + workflowId))
                              .workflowState().onEvent(eventMessage, processingContext);
        } else {
            // handle starting of new processes
            checkAndCreateNewWorkflow(eventMessage, processingContext);
            // route external events to workflows waiting for them
            for (var handle : workflowRepository.findAll()) {
                // TODO: discussion regarding hibernating workflows ->
                // TODO: is it safe to put an eventMessage in the queue?
                handle.workflowState().onEvent(eventMessage, processingContext);
            }
        }

        return MessageStream.empty();
    }

    /**
     * This is a place to be called from Event Processor
     */
    public void runWorkflows() {
        logger.debug("Executing {} workflows.", workflowRepository.findAll().size());
        for (var handle : workflowRepository.findAll()) {
            try {
                handle.workflowState().execute(handle.workflowConfiguration(), handle.workflowContext());
            } catch (Throwable t) {
                throw new RuntimeException("Error during workflow execution", t);
            }
        }
    }

    private void checkAndCreateNewWorkflow(@Nonnull EventMessage eventMessage,
                                           @Nonnull ProcessingContext processingContext) {
        var definitions = workflowDefinitionRegistry.getWorkflowsConfigurations(eventMessage.type().qualifiedName());
        definitions.forEach(predicatedWorkflowConfiguration -> {

                                if (predicatedWorkflowConfiguration.predicate().test(eventMessage)) {

                                    var workflowConfiguration = predicatedWorkflowConfiguration.configuration();

                                    var payload = Objects.requireNonNull(eventMessage.payloadAs(
                                            new TypeReference<Map<String, Object>>() {
                                            },
                                            processingContext.component(Converter.class)
                                    ), "Error converting initial payload");
                                    var workflowId = workflowConfiguration.associationProvider().apply(payload)
                                                                          .orElseThrow(() -> new IllegalArgumentException(
                                                                                  "Could not extract workflow id from payload " + payload
                                                                                          + " for workflow definition "
                                                                                          + workflowConfiguration.workflowDefinition())
                                                                          );

                                    var workflowContext = workflowConfiguration.workflowContextFactory()
                                                                               .createContext(payload, workflowId, processingContext);

                                    // avoid multiple workflows for the same workflow id.
                                    workflowRepository.save(() -> {
                                        logger.info("Starting new workflow with '{}'", eventMessage.payload());
                                        var workflowState = workflowConfiguration.workflowStateFactory().create(workflowContext);
                                        return new WorkflowExecution(workflowId, workflowConfiguration, workflowContext, workflowState);
                                    });
                                }
                            }
        );
    }

    public Set<WorkflowExecution> workflowInstances() {
        return workflowRepository.findAll();
    }

    public void shutdown() {
        workflowRepository.clear();
    }
}
