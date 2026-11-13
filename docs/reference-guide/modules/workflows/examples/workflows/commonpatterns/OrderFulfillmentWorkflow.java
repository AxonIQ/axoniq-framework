/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.commonpatterns;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.annotation.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

public class OrderFulfillmentWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(OrderFulfillmentWorkflow.class);

    // tag::parent[]
    @Workflow(idProperty = "orderId",
            startOnEventClass = OrderPlacedEvent.class,
            workflowNamespace = "io.myapp")
    public void execute(SimpleWorkflowContext workflowContext) {

        // ... do some work ...

        var orderId = (String) workflowContext.workflowPayload().get("orderId");
        var amount = ((Number) workflowContext.workflowPayload().get("amount")).doubleValue();

        // Register the wait for the child's completion BEFORE launching the child.
        var completed = workflowContext.waitForEvent("awaitPaymentProcess",  // <1>
                                         PaymentProcessCompleted.class,
                                         associate(payloadProperty("orderId"),
                                                   equalsTo(orderId)),       // <2>
                                         step -> step.timeout(Duration.ofMinutes(30)));

        // Launch the child workflow by publishing a dedicated event.
        workflowContext.awaitPublish("paymentProcessStarted",                  // <3>
                         new PaymentProcessStarted("payment-" + orderId, orderId, amount));

        // ... do other work in parallel while child runs ...

        completed.await();                                                    // <4>
        logger.info("Child workflow completed for order {}", orderId);
    }
    // end::parent[]
}
