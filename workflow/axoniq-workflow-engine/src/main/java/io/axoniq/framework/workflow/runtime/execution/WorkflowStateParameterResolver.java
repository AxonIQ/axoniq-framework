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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;

/**
 * Parameter resolver responsible for resolving the workflow state based on the workflow id. It will first try to find
 * it in the workflow execution repository (runtime) and then consult the workflow history repository.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class WorkflowStateParameterResolver implements ParameterResolver<WorkflowState> {

    private final Configuration configuration;

    public WorkflowStateParameterResolver(Configuration configuration) {
        this.configuration = requireNonNull(configuration, "The Configuration is required");
    }

    @Override
    public CompletableFuture<WorkflowState> resolveParameterValue(ProcessingContext context) {
        Message message = requireNonNullElseGet(Message.fromContext(context), GenericMessage::emptyMessage);
        if (MetadataUtils.hasWorkflowId().test(message.metadata())) {
            var workflowId = MetadataUtils.getWorkflowId(message.metadata());
            var state = configuration
                    .getComponent(WorkflowExecutionRepository.class)
                    .findById(workflowId).map(WorkflowExecution::state)
                    .orElseGet(
                            () -> configuration
                                    .getComponent(WorkflowHistoryRepository.class)
                                    .findById(workflowId).map(WorkflowHistory::state)
                                    .orElseThrow(
                                            () -> new IllegalStateException(
                                                    "Unable to inject workflow state, since no workflow id was found in the message.")
                                    )
                    );
            return CompletableFuture.completedFuture(state);
        } else {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Unable to inject workflow state, since no workflow id was found in the message."));
        }
    }

    @Override
    public boolean matches(ProcessingContext context) {
        return Message.fromContext(context) != null
                && MetadataUtils.hasWorkflowId().test(requireNonNull(Message.fromContext(context)).metadata());
    }
}
