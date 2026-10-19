/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.commonpatterns;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.annotation.Workflow;

public class PaymentProcessWorkflow {

    // tag::child[]
    @Workflow(idProperty = "childWorkflowId",
              startOnEventClass = PaymentProcessStarted.class,                 // <1>
              workflowNamespace = "io.myapp.payments")
    public void execute(SimpleWorkflowContext workflowContext) {

        workflowContext.awaitExecute("validatePayment",
                workflowContext.workflowPayload(),
                PaymentService::validate);

        workflowContext.awaitExecute("chargeCustomer",
                workflowContext.workflowPayload(),
                PaymentService::charge);

        // Signal the parent explicitly
        workflowContext.awaitPublish("notifyParent",                         // <2>
                new PaymentProcessCompleted(
                        (String) workflowContext.workflowPayload().get("orderId"),
                        (String) workflowContext.workflowPayload().get("childWorkflowId")));
    }
    // end::child[]
}
