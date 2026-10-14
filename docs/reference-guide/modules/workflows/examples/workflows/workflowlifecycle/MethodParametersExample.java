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
