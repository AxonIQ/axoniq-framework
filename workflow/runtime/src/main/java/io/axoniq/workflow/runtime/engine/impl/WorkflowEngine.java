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

import io.axoniq.workflow.runtime.api.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.engine.execution.WorkflowInstance;
import io.axoniq.workflow.runtime.engine.execution.WorkflowInstanceRepository;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class WorkflowEngine implements EventHandler {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private final WorkflowInstanceRepository workflowInstanceRepository;

    public WorkflowEngine(
            @Nonnull WorkflowConfigurationRegistry<?> workflowConfigurationRegistry,
            @Nonnull WorkflowInstanceRepository workflowInstanceRepository
    ) {
        this.workflowConfigurationRegistry = workflowConfigurationRegistry;
        this.workflowInstanceRepository = workflowInstanceRepository;
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
            var instance = workflowInstanceRepository.findById(workflowId)
                                      .orElseThrow(() -> new IllegalStateException("No workflow found for id: " + workflowId));
            instance.workflowExecution().onEvent(eventMessage, processingContext);
        } else {
            // handle starting of new processes
            checkAndCreateNewWorkflow(eventMessage, processingContext);
            // route external events to workflows waiting for them
            for (var instance : workflowInstanceRepository.findAll()) {
                // TODO: discussion regarding hibernating workflows ->
                // TODO: is it safe to put an eventMessage in the queue?
                instance.workflowExecution().onEvent(eventMessage, processingContext);
            }
        }

        return MessageStream.empty();
    }

    /**
     * This is a place to be called from Event Processor
     */
    public void runWorkflows() {
        logger.debug("Executing {} workflows.", workflowInstanceRepository.findAll().size());
        for (var instances : workflowInstanceRepository.findAll()) {
            try {
                instances.workflowExecution().execute(instances.workflowContext());
            } catch (Throwable t) {
                throw new RuntimeException("Error during workflow execution", t);
            }
        }
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

                                    var workflowContext = workflowConfiguration.workflowContextFactory()
                                                                               .createContext(payload, workflowId, processingContext, workflowConfiguration);

                                    // avoid multiple workflows for the same workflow id.
                                    workflowInstanceRepository.save(() -> {
                                        logger.info("Starting new workflow with '{}'", eventMessage.payload());
                                        var workflowState = workflowConfiguration.workflowStateFactory().create(workflowContext);
                                        return new WorkflowInstance(workflowId, workflowConfiguration, workflowContext, workflowState);
                                    });
                                }
                            }
        );
    }

    public Set<WorkflowInstance> workflowInstances() {
        return workflowInstanceRepository.findAll();
    }

    public void shutdown() {
        workflowInstanceRepository.clear();
    }
}
