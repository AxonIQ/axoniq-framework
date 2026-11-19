/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.steporchestration;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

import java.time.Duration;

import static io.axoniq.framework.workflow.dsl.api.Payload.payload;

public class CustomerNotificationWorkflow {

    // tag::customer-notification-workflow[]
    @Workflow(
            idProperty = "customerId",
            startOnEventClass = OrderPlacedEvent.class
    )
    public void execute(SimpleWorkflowContext context) {
        context.awaitExecute(
                "sendConfirmationEmail",
                payload("email", context.workflowPayload().get("email"),
                        "orderId", context.workflowId()).getValues(),
                EmailService::sendOrderConfirmation
        );

        context.awaitSleep("waitBeforeFollowUp", Duration.ofDays(3));

        context.awaitExecute(
                "sendFollowUpEmail",
                payload("email", context.workflowPayload().get("email"),
                        "orderId", context.workflowId()).getValues(),
                EmailService::sendFollowUp
        );
    }
    // end::customer-notification-workflow[]
}
