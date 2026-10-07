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

        context.sleep("waitBeforeFollowUp", Duration.ofDays(3));

        context.awaitExecute(
                "sendFollowUpEmail",
                payload("email", context.workflowPayload().get("email"),
                        "orderId", context.workflowId()).getValues(),
                EmailService::sendFollowUp
        );
    }
    // end::customer-notification-workflow[]
}
