/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.configuration;

import java.time.Duration;

public class PurchaseApprovalWorkflow {

    // tag::purchase-approval-workflow[]
    public void execute(ApprovalWorkflowContext workflowContext) {
        if (!workflowContext.requestApproval("team-lead", Duration.ofDays(3))) {
            workflowContext.escalate("department-head");
        }
        workflowContext.notifyOutcome(workflowContext.requester(), "approved");
    }
    // end::purchase-approval-workflow[]
}
