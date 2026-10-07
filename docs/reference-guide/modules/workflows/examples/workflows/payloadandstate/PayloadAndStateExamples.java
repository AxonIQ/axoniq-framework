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

package workflows.payloadandstate;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

import java.time.Duration;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.payloadProperty;
import static io.axoniq.framework.workflow.dsl.api.Payload.payload;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;

public class PayloadAndStateExamples {

    public void readPayload(SimpleWorkflowContext context) {
        // tag::read-payload[]
        var payload = context.workflowPayload(); // <1>
        var orderId = payload.get("orderId");
        var email = payload.get("email");
        // end::read-payload[]
    }

    public void setPayload(SimpleWorkflowContext context) {
        // tag::set-payload[]
        var confirmation = context.awaitEvent(
                "awaitPayment",
                PaymentConfirmed.class,
                associate(payloadProperty("orderId"),
                          equalsTo(context.workflowPayload().get("orderId"))),
                step -> step.timeout(Duration.ofMinutes(15))
        );

        context.setPayload("storePaymentDetails", confirmation); // <1>
        // end::set-payload[]
    }

    public void passPayloadToStep(SimpleWorkflowContext context) {
        // tag::pass-payload[]
        context.awaitExecute(
                "shipOrder",
                context.workflowPayload(), // <1>
                ShippingService::shipOrder,
                step -> step.timeout(Duration.ofMinutes(5))
        );
        // end::pass-payload[]
    }

    public void payloadHelper(SimpleWorkflowContext context) {
        // tag::payload-helper[]
        var email = payload(context.workflowPayload()).get("email"); // <1>
        // end::payload-helper[]
    }
}
