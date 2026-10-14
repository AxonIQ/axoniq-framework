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

package workflows.configuration;

import io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.Payload.payload;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;

// tag::approval-workflow-context[]
public class ApprovalWorkflowContext extends SimpleWorkflowContext {

    public ApprovalWorkflowContext(
            String workflowId,
            Map<String, @Nullable Object> payload,
            ProcessingContext processingContext,
            WorkflowConfiguration<?> workflowConfiguration
    ) {
        super(workflowId, payload, processingContext, workflowConfiguration);
    }

    /**
     * Request approval and wait for a response event. Returns true if approved.
     */
    public boolean requestApproval(String approver, Duration deadline) {
        awaitExecute("requestApproval",
                     payload("approver", approver, "requestId", workflowId()).getValues(),
                     ApprovalService::sendRequest);

        var decision = awaitEvent(
                "awaitDecision",
                ApprovalDecisionEvent.class,
                associate(EventAssociationsUtils.payloadProperty("requestId"),
                          EventAssociationsUtils.equalsTo(workflowId())),
                step -> step.timeout(deadline)
        );
        return "approved".equals(decision.outcome());
    }

    /**
     * Escalate to a manager when the original approver doesn't respond in time.
     */
    public void escalate(String manager) {
        awaitExecute("escalate",
                     payload("manager", manager, "requestId", workflowId()).getValues(),
                     ApprovalService::escalate);
    }

    /**
     * Notify the requester of the final outcome.
     */
    public void notifyOutcome(String recipient, String decision) {
        awaitExecute("notifyOutcome",
                     payload("recipient", recipient, "decision", decision).getValues(),
                     NotificationService::sendDecision);
    }

    /**
     * Domain accessors for the workflow payload.
     */
    public String requester() {
        return (String) workflowPayload().get("requester");
    }

    public String department() {
        return (String) workflowPayload().get("department");
    }

    public double amount() {
        return (double) workflowPayload().get("amount");
    }
}
// end::approval-workflow-context[]
