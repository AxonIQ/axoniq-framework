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

package gettingstarted.order.moresteps;

import gettingstarted.order.InventoryService;
import gettingstarted.order.PaymentService;
import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.Payload.payload;
import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.baseName;

public class OrderFulfillmentWorkflow {

    // tag::execute-method[]
    @Workflow(idProperty = "orderId", startOnEventName = "OrderPlaced")
    public void execute(SimpleWorkflowContext workflowContext) {

        var customerId = workflowContext.workflowPayload().get("customerId");
        var amount = workflowContext.workflowPayload().get("amount");

        var reservation = workflowContext.awaitExecute("reserveStock",
                payload("customerId", customerId, "amount", amount).getValues(),
                (pc, input) -> Map.of("reserved", InventoryService.reserveStock(input)));
        if (!Boolean.TRUE.equals(reservation.get("reserved"))) {
            workflowContext.fail(new RuntimeException("Stock unavailable"));
            return;
        }

        workflowContext.awaitExecute("initiatePayment",             // <1>
                         payload("customerId", customerId,
                                 "amount", amount).getValues(),
                         PaymentService::initiatePayment,
                         step -> step.timeout(Duration.ofSeconds(30))
                                     .eventNameCustomizer(baseName("InitiatingPaymentForCustomer")));  // <2>
    }
    // end::execute-method[]
}
