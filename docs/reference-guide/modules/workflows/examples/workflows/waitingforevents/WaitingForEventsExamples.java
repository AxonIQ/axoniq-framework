/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.waitingforevents;

import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.payloadProperty;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;

public class WaitingForEventsExamples {

    private static final Logger logger = LoggerFactory.getLogger(WaitingForEventsExamples.class);

    public void awaitEvent(SimpleWorkflowContext context) {
        // tag::await-event[]
        var confirmation = context.awaitEvent(
                "awaitPayment",
                PaymentConfirmed.class, // <1>
                associate(payloadProperty("orderId"),
                          equalsTo(context.workflowPayload().get("orderId"))),  // <2>
                step -> step.timeout(Duration.ofMinutes(15))    // <3>
        );
        // end::await-event[]
    }

    public void filteringAssociations(SimpleWorkflowContext context) {
        // tag::filtering-associations[]
        var confirmation = context.awaitEvent(
                "awaitPayment",
                PaymentConfirmed.class,
                associate(  // <1>
                        payloadProperty("orderId"), // <2>
                        equalsTo(context.workflowPayload().get("orderId"))  // <3>
                ),
                step -> step.timeout(Duration.ofMinutes(15))
        );
        // end::filtering-associations[]
    }

    public void timeoutCheck(SimpleWorkflowContext context) {
        // tag::timeout-check[]
        var stepResult = context.waitForEvent(
                "awaitPayment",
                PaymentConfirmed.class,
                associate(payloadProperty("orderId"),
                          equalsTo(context.workflowPayload().get("orderId"))),
                step -> step.timeout(Duration.ofMinutes(15))
        );
        if (stepResult.success()) {
            // handle success
        } else if (stepResult.timeout()) {
            // handle timeout
        }
        // end::timeout-check[]
    }

    public void multipleEvents(SimpleWorkflowContext context) {
        // tag::multiple-events[]
        var payment = context.waitForEvent(
                "awaitPayment", // <1>
                PaymentConfirmed.class,
                associate(payloadProperty("orderId"),
                          equalsTo(context.workflowPayload().get("orderId"))),
                step -> step.timeout(Duration.ofMinutes(15))
        );

        var approval = context.waitForEvent(
                "awaitApproval",    // <1>
                ManagerApproved.class,
                associate(payloadProperty("orderId"),
                          equalsTo(context.workflowPayload().get("orderId"))),
                step -> step.timeout(Duration.ofMinutes(30))
        );

        context.allMatch(
                WorkflowStepResult::success,
                payment, approval // <2>
        ).await();

        logger.info("Both payment and approval received, proceeding!");
        // end::multiple-events[]
    }

    public void sleepBasic(SimpleWorkflowContext context) {
        // tag::sleep-basic[]
        context.sleep("cooldownPeriod", Duration.ofDays(10)); // <1>
        // end::sleep-basic[]
    }

    public void nonBlockingSleep(SimpleWorkflowContext context) {
        // tag::non-blocking-sleep[]
        var cooldown = context.sleep("cooldown", step -> step.timeout(Duration.ofMinutes(5)));
        var approval = context.waitForEvent(
                "approval",
                ManagerApproved.class,
                associate(payloadProperty("orderId"),
                          equalsTo(context.workflowPayload().get("orderId"))),
                step -> step.timeout(Duration.ofHours(1))
        );

        // Proceed when either the cooldown finishes OR approval arrives
        context.anyMatch(
                WorkflowStepResult::isCompleted,
                cooldown, approval
        ).await();
        // end::non-blocking-sleep[]
    }
}
