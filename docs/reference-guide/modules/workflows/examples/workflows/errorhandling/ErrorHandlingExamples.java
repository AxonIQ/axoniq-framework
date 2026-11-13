/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.errorhandling;

import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.WorkflowExecutionException;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.RecoverableWorkflowExceptionPolicy;

import java.time.Duration;

import static io.axoniq.framework.workflow.dsl.api.Payload.payload;

public class ErrorHandlingExamples {

    public void catchStepFailure(SimpleWorkflowContext context,
                                 Object customerId,
                                 Object amount) {
        // tag::catch-step-failure[]
        try {
            context.awaitExecute("chargePayment",
                                 payload("customerId", customerId, "amount", amount).getValues(),
                                 PaymentService::charge);
        } catch (StepFailedException e) {
            context.awaitExecute("releaseStock",
                                 payload("customerId", customerId).getValues(),
                                 InventoryService::releaseStock);
            context.fail(new RuntimeException("Payment failed: " + e.getMessage())); // <1>
        }
        // end::catch-step-failure[]
    }

    public void nonBlockingCheck(SimpleWorkflowContext context, Object customerId) {
        // tag::non-blocking-check[]
        var shipping = context.execute("shipOrder", context.workflowPayload(),
                                       ShippingService::shipOrder,
                                       step -> step.timeout(Duration.ofMinutes(5)));

        if (shipping.failure()) { // blocks until the step reaches a terminal status
            context.awaitExecute("refundPayment",
                                 payload("customerId", customerId).getValues(),
                                 PaymentService::refund);
            context.fail(new RuntimeException(
                    "Shipping failed: " + shipping.error().get().getMessage()
            ));
        }
        // end::non-blocking-check[]
    }

    public void typeNameBranch() {
        try {
            // ...
            // tag::type-name-branch[]
        } catch (StepFailedException e) {
            var cause = (WorkflowExecutionException) e.getCause();
            if (cause.isType(OutOfStockException.class)) {
                // ...
            }
        }
        // end::type-name-branch[]
    }

    public void recoverablePolicyExtend() {
        // tag::recoverable-policy-extend[]
        RecoverableWorkflowExceptionPolicy policy =
                RecoverableWorkflowExceptionPolicy.DEFAULT.or(e -> e instanceof BackendUnavailableException);
        // end::recoverable-policy-extend[]
    }

    public void recoverablePolicyReplace() {
        // tag::recoverable-policy-replace[]
        RecoverableWorkflowExceptionPolicy policy = e -> e instanceof BackendUnavailableException;
        // end::recoverable-policy-replace[]
    }
}
