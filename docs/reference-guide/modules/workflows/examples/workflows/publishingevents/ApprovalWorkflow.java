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

package workflows.publishingevents;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;

import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

public class ApprovalWorkflow {

    private final Inventory inventory = new Inventory();

    @Workflow(idProperty = "orderId",
            startOnEventClass = OrderPlaced.class,
            workflowNamespace = "io.myapp")
    public void execute(SimpleWorkflowContext workflowContext) {
        String orderId = (String) workflowContext.workflowPayload().get("orderId");
        String approvedBy = "alice";

        // tag::await-publish[]
        workflowContext.awaitPublish("notifyApproved",                      // <1>
                         new OrderApproved(orderId, approvedBy));                    // <2>
        // end::await-publish[]
    }

    public void requestQuote(SimpleWorkflowContext workflowContext) {
        String orderId = (String) workflowContext.workflowPayload().get("orderId");

        // tag::wait-then-publish[]
        var reply = workflowContext.waitForEvent("awaitQuote", QuoteReceived.class,
                                     associate(payloadProperty("orderId"), equalsTo(orderId)));
        workflowContext.awaitPublish("requestQuote", new QuoteRequested(orderId));
        reply.await();
        // end::wait-then-publish[]
    }

    public void approveAndReserve(SimpleWorkflowContext workflowContext) {
        String orderId = (String) workflowContext.workflowPayload().get("orderId");
        String approvedBy = "alice";

        // tag::non-blocking[]
        var notified = workflowContext.publish("notifyApproved", new OrderApproved(orderId, approvedBy));
        var reserved = workflowContext.execute("reserveStock", Map.of(), (pc, p) -> inventory.reserve(p));
        workflowContext.allMatch(WorkflowStepResult::success, notified, reserved).await();
        // end::non-blocking[]
    }
}
