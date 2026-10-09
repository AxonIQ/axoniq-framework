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
