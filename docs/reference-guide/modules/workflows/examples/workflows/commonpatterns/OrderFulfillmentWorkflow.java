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

package workflows.commonpatterns;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

public class OrderFulfillmentWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(OrderFulfillmentWorkflow.class);

    // tag::parent[]
    @Workflow(idProperty = "orderId",
              startOnEventClass = OrderPlacedEvent.class,
              workflowNamespace = "io.myapp")
    public void execute(SimpleWorkflowContext ctx) {

        // ... do some work ...

        var orderId = (String) ctx.workflowPayload().get("orderId");
        var amount = ((Number) ctx.workflowPayload().get("amount")).doubleValue();

        // Register the wait for the child's completion BEFORE launching the child.
        var completed = ctx.waitForEvent("awaitPaymentProcess",              // <1>
                PaymentProcessCompleted.class,
                associate(payloadProperty("orderId"), equalsTo(orderId)),       // <2>
                step -> step.timeout(Duration.ofMinutes(30)));

        // Launch the child workflow by publishing a dedicated event.
        ctx.awaitPublish("paymentProcessStarted",                              // <3>
                new PaymentProcessStarted("payment-" + orderId, orderId, amount));

        // ... do other work in parallel while child runs ...

        completed.await();                                                    // <4>
        logger.info("Child workflow completed for order {}", orderId);
    }
    // end::parent[]
}
