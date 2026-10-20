/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.workflowlifecycle;

import io.axoniq.framework.workflow.annotation.WorkflowCompletedHandler;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MethodParametersExample {

    private static final Logger logger = LoggerFactory.getLogger(MethodParametersExample.class);

    // tag::method-parameters-example[]
    @WorkflowCompletedHandler
    public void onCompleted(WorkflowStatus status,
                            SimpleWorkflowContext context,
                            WorkflowState state) {
        logger.info("Order {} fulfilled successfully! (status: {})",
                    context.workflowPayload().get("orderId"),
                    state.workflowStatus());
    }
    // end::method-parameters-example[]
}
