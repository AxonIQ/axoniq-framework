/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.configuration;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Map;

// tag::approval-workflow-context-factory[]
public class ApprovalWorkflowContextFactory
        implements WorkflowContextFactory<ApprovalWorkflowContext> {

    @Override
    public ApprovalWorkflowContext createContext(
            Map<String, @Nullable Object> payload,
            String workflowId,
            ProcessingContext processingContext,
            WorkflowConfiguration<?> workflowConfiguration
    ) {
        return new ApprovalWorkflowContext(
                workflowId, payload, processingContext, workflowConfiguration
        );
    }
}
// end::approval-workflow-context-factory[]
