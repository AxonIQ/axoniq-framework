/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.workflowlifecycle;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.annotation.WorkflowCancelledHandler;
import io.axoniq.framework.workflow.annotation.WorkflowCompletedHandler;
import io.axoniq.framework.workflow.annotation.WorkflowFailedHandler;
import io.axoniq.framework.workflow.annotation.WorkflowTimedOutHandler;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// tag::annotation-listeners[]
public class OrderFulfillmentWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(OrderFulfillmentWorkflow.class);

    @Workflow(
            idProperty = "orderId",
            startOnEventClass = OrderPlacedEvent.class
    )
    public void execute(SimpleWorkflowContext context) {
        // ... workflow logic
    }

    @WorkflowCompletedHandler // <1>
    public void onCompleted(WorkflowStatus status,
                            SimpleWorkflowContext context) {
        logger.info("Order {} fulfilled successfully!", context.workflowPayload().get("orderId"));
    }

    @WorkflowFailedHandler // <2>
    public void onFailed(WorkflowStatus status,
                         SimpleWorkflowContext context) {
        logger.warn("Order {} failed: {}", context.workflowPayload().get("orderId"), status);
    }

    @WorkflowCancelledHandler // <3>
    public void onCancelled(WorkflowStatus status,
                            SimpleWorkflowContext context) {
        logger.info("Order {} was cancelled", context.workflowPayload().get("orderId"));
    }

    @WorkflowTimedOutHandler // <4>
    public void onTimedOut(WorkflowStatus status,
                           SimpleWorkflowContext context) {
        logger.warn("Order {} timed out", context.workflowPayload().get("orderId"));
    }
}
// end::annotation-listeners[]
