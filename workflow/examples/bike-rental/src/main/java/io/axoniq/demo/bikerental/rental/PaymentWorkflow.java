/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.demo.bikerental.rental;

import io.axoniq.demo.bikerental.coreapi.payment.PaymentConfirmedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentPreparedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentRejectedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PreparePaymentCommand;
import io.axoniq.demo.bikerental.coreapi.payment.RejectPaymentCommand;
import io.axoniq.demo.bikerental.coreapi.rental.ApproveRequestCommand;
import io.axoniq.demo.bikerental.coreapi.rental.RejectRequestCommand;
import io.axoniq.workflow.dsl.Payload;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static io.axoniq.workflow.dsl.Payload.payload;
import static io.axoniq.workflow.dsl.simple.SimpleWorkflowContext.equalsTo;
import static io.axoniq.workflow.runtime.engine.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.defaults;
import static io.axoniq.workflow.runtime.engine.util.AssociationsUtils.associate;

public class PaymentWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(PaymentWorkflow.class);
    private final CommandGateway commandGateway;

    public PaymentWorkflow(CommandGateway commandGateway) {
        this.commandGateway = commandGateway;
    }

    @Workflow(
            startOnEvent = "io.axoniq.demo.bikerental.coreapi.rental.BikeRequestedEvent",
            idProperty = "bikeId",
            workflowName = "PaymentWorkflow"
    )
    public void execute(SimpleWorkflowContext ctx) {

        ctx.setPayload("setAmountAndReference",
                       Map.of(
                               "amount", 10,
                               "paymentReference", ctx.workflowPayload().get("rentalReference")
                       ));

        var paymentReference = ctx.workflowPayload().get("paymentReference");

        AtomicBoolean paymentPending = new AtomicBoolean(true);
        while (paymentPending.get()) {

            logger.info("Preparing payment {}", paymentReference);

            var paymentPrepared = ctx.waitFor("paymentPrepared",
                                              PaymentPreparedEvent.class,
                                              associate(payloadProperty("paymentReference"),
                                                        equalsTo(paymentReference)),
                                              Duration.ofSeconds(10)
            );

            if (sendCommand(ctx,
                            "preparePayment",
                            payload ->
                                    new PreparePaymentCommand(
                                            payload.get("amount"),
                                            payload.get("paymentReference")
                                    )
            ).success()) {
                paymentPrepared.await();
                if (paymentPrepared.failure()) {
                    // NICE TO HAVE
                    // paymentPrepared.cancel("Prepare payment failed.");
                    ctx.sleep("retryPayment", Duration.ofSeconds(5));
                } else if (paymentPrepared.success()) {
                    paymentPending.set(false);
                    var paymentId = paymentPrepared.result().orElseThrow(() -> new IllegalStateException("No payload"));
                    ctx.setPayload("setPaymentId", paymentId);

                    logger.info("Payment prepared successfully for reference {}, the payment id is {}",
                                paymentReference,
                                paymentId);
                }
            } else {
                // NICE TO HAVE
                // paymentPrepared.cancel("Prepare payment failed.");
                ctx.sleep("retryPayment", Duration.ofSeconds(5));
            }
        }

        var timeout = 240;
        logger.info("Waiting for payment confirmation or rejection for the next {} seconds", timeout);
        var paymentStatus = ctx.anyMatch(
                WorkflowStepResult::success,
                ctx.waitFor("paymentConfirmed",
                            PaymentConfirmedEvent.class,
                            associate(payloadProperty("paymentReference"), equalsTo(paymentReference)),
                            Duration.ofSeconds(timeout)
                ),
                ctx.waitFor("paymentRejected",
                            PaymentRejectedEvent.class,
                            associate(payloadProperty("paymentReference"), equalsTo(paymentReference)),
                            Duration.ofSeconds(timeout)
                )
        );
        paymentStatus.await();

        if (!paymentStatus.matched().isEmpty()) {
            switch (paymentStatus.matched().getFirst().getStepName()) {
                case "paymentConfirmed":
                    logger.info("Payment confirmed. Approving the request.");
                    sendCommand(ctx,
                                "confirmRequest",
                                payload -> new ApproveRequestCommand(
                                        payload.get("bikeId"),
                                        payload.get("renter")
                                )
                    ).await();
                    break;
                case "paymentRejected":
                    logger.info("Payment rejected. Rejecting the request.");
                    sendCommand(ctx,
                                "confirmRequest",
                                payload -> new RejectRequestCommand(
                                        payload.get("bikeId"),
                                        payload.get("renter")
                                )
                    ).await();
                    break;
            }
        } else {
            logger.info("Payment not confirmed or rejected within {} seconds. Rejecting the request.", timeout);
            // timeout
            sendCommand(
                    ctx,
                    "rejectPayment",
                    payload -> new RejectPaymentCommand(
                            payload.get("paymentId")
                    )
            ).await();
        }
    }

    private WorkflowStepResult sendCommand(
            SimpleWorkflowContext ctx,
            String stepName,
            Function<Payload, Object> commandSupplier) {
        return ctx.execute(
                stepName,
                ctx.workflowPayload(),
                (pc, p) -> {
                    var payload = payload(p);
                    this.commandGateway.sendAndWait(
                            commandSupplier.apply(payload)
                    );
                    return Map.of();
                },
                Duration.ofSeconds(5),
                defaults()
        );
    }
}
