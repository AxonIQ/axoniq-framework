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

import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.payloadProperty;
import static io.axoniq.framework.workflow.dsl.api.Payload.payload;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;

@SuppressWarnings("unchecked")
public class CommonPatternsExamples {

    private static final Logger logger = LoggerFactory.getLogger(CommonPatternsExamples.class);

    public void fanOutFanIn(SimpleWorkflowContext context) {
        // tag::fan-out[]
        var lineItems = (List<Map<String, Object>>) context.workflowPayload().get("lineItems");

        // Fan-out: launch one reservation per line item
        var reservations = lineItems.stream().map(
                item -> context.execute("reserve-" + item.get("sku"), // <1>
                                        payload("sku", item.get("sku"),
                                                "quantity", item.get("quantity")).getValues(),
                                        InventoryService::reserveStock,
                                        step -> step.timeout(Duration.ofSeconds(30)))
        ).toArray(WorkflowStepResult[]::new);
        // end::fan-out[]

        // tag::fan-in[]
        // Fan-in: wait for all reservations
        var guard = context.allMatch(WorkflowStepResult::success, reservations); // <1>

        if (!guard.success()) {
            // Some reservations failed, cancel the rest and compensate
            for (var step : guard.unmatched()) {
                if (!step.isCompleted()) {
                    step.cancel("Partial reservation failure");
                }
            }
            for (var step : guard.matched()) {
                if (step.success()) {
                    context.awaitExecute("release-" + step.getStepName(),
                                         step.<Map<String, Object>>result().orElse(Map.of()),
                                         InventoryService::releaseStock);
                }
            }
            context.fail(new RuntimeException("Not all items could be reserved"));
        }

        logger.info("All {} items reserved!", reservations.length);
        // end::fan-in[]
    }

    public void fanOutWaitForEvent(SimpleWorkflowContext context) {
        // tag::fan-out-waitforevent[]
        var approvers = List.of("team-lead", "finance", "legal");

        // Fan-out: send approval requests and wait for each response
        for (var approver : approvers) {
            context.awaitExecute("requestApproval-" + approver,
                                 payload("approver", approver, "requestId", context.workflowId()).getValues(),
                                 ApprovalService::sendRequest);
        }

        var approvals = approvers.stream().map(
                approver -> context.waitForEvent(
                        "approval-" + approver, // <1>
                        ApprovalDecisionEvent.class,
                        associate(payloadProperty("approver"), equalsTo(approver)),
                        step -> step.timeout(Duration.ofDays(5))
                )
        ).toArray(WorkflowStepResult[]::new);

        // Fan-in: wait for all approvals
        var result = context.allMatch(WorkflowStepResult::success, approvals);

        if (!result.success()) {
            context.fail(new RuntimeException("Not all approvals received within deadline"));
        }

        logger.info("All {} approvers responded!", approvers.size());
        // end::fan-out-waitforevent[]
    }

    public void saga(SimpleWorkflowContext ctx, String orderId, double amount) {
        // tag::saga[]
        // Step 1: reserve stock
        ctx.awaitExecute("reserveStock",
                         payload("orderId", orderId).getValues(),
                         InventoryService::reserveStock);

        // Step 2: charge payment
        try {
            ctx.awaitExecute("chargePayment",
                             payload("orderId", orderId, "amount", amount).getValues(),
                             PaymentService::charge);
        } catch (StepFailedException e) {
            // Payment failed, compensate step 1
            ctx.awaitExecute("releaseStock",
                             payload("orderId", orderId).getValues(),
                             InventoryService::releaseStock);
            ctx.fail(new RuntimeException("Payment failed: " + e.getMessage()));
        }

        // Step 3: ship order
        try {
            ctx.awaitExecute("shipOrder",
                             payload("orderId", orderId).getValues(),
                             ShippingService::shipOrder);
        } catch (StepFailedException e) {
            // Shipping failed, compensate steps 2 and 1 in reverse
            ctx.awaitExecute("refundPayment",
                             payload("orderId", orderId, "amount", amount).getValues(),
                             PaymentService::refund);
            ctx.awaitExecute("releaseStock",
                             payload("orderId", orderId).getValues(),
                             InventoryService::releaseStock);
            ctx.fail(new RuntimeException("Shipping failed: " + e.getMessage()));
        }
        // end::saga[]
    }

    public void scatterGather(SimpleWorkflowContext ctx, String itemId) {
        // tag::scatter-gather[]
        var suppliers = List.of("supplierA", "supplierB", "supplierC");

        // Scatter: request quotes from all suppliers
        var quotes = suppliers.stream().map(
                s -> ctx.execute("getQuote-" + s,
                                 payload("supplier", s, "itemId", itemId).getValues(),
                                 QuoteService::requestQuote,
                                 step -> step.timeout(Duration.ofSeconds(30)))
        ).toList();

        var remaining = new ArrayList<>(quotes);

        // Gather: process quotes as they arrive
        while (!remaining.isEmpty()) {
            var fastest = ctx.anyMatch(WorkflowStepResult::isCompleted,
                                       remaining.toArray(WorkflowStepResult[]::new));
            fastest.await();

            var winner = fastest.matched().getFirst();
            logger.info("Received quote from {}: {}", winner.getStepName(), winner.result());

            remaining.remove(winner); // <1>
        }
        // end::scatter-gather[]
    }

    public void humanInTheLoop(SimpleWorkflowContext ctx) {
        // tag::human-in-the-loop[]
        // Request approval from the team lead
        ctx.awaitExecute("requestApproval",
                         payload("approver", "team-lead", "requestId", ctx.workflowId()).getValues(),
                         ApprovalService::sendRequest);

        // Wait for their response
        var decision = ctx.waitForEvent("awaitApproval",
                                        ApprovalDecisionEvent.class,
                                        associate(payloadProperty("requestId"), equalsTo(ctx.workflowId())),
                                        step -> step.timeout(Duration.ofDays(3)));

        if (decision.timeout()) {   // <1>
            // Team lead didn't respond, escalate to department head
            ctx.awaitExecute("escalate",
                             payload("approver", "department-head", "requestId", ctx.workflowId()).getValues(),
                             ApprovalService::escalate);

            decision = ctx.waitForEvent("awaitEscalatedApproval",
                                        ApprovalDecisionEvent.class,
                                        associate(payloadProperty("requestId"), equalsTo(ctx.workflowId())),
                                        step -> step.timeout(Duration.ofDays(1)));

            if (decision.timeout()) {
                ctx.fail(new RuntimeException("No approval received after escalation"));
            }
        }

        decision.await();
        logger.info("Approval decision received: {}", decision.result());
        // end::human-in-the-loop[]
    }

    public void circuitBreaker(SimpleWorkflowContext ctx, String email, String subject) {
        // tag::circuit-breaker[]
        var failureCount = (int) ctx.workflowPayload().getOrDefault("emailFailures", 0);

        if (failureCount < 3) { // <1>
            try {
                ctx.awaitExecute("sendEmail",
                                 payload("to", email, "subject", subject).getValues(),
                                 EmailService::send);
                ctx.setPayload("resetEmailFailures",
                               Map.of("emailFailures", 0)); // <2>
            } catch (StepFailedException e) {
                ctx.setPayload("recordEmailFailure",
                               Map.of("emailFailures", failureCount + 1));  // <3>
                logger.warn("Email failed ({}/3), will skip next time", failureCount + 1);
            }
        } else {
            logger.warn("Circuit open, skipping email, {} consecutive failures", failureCount);
        }
        // end::circuit-breaker[]
    }

    public void scheduledDelayedSleep(SimpleWorkflowContext ctx, String email) {
        // tag::scheduled-delayed-sleep[]
        // Wait until a specific time
        var targetTime = LocalDateTime.of(2026, 4, 1, 9, 0);
        var delay = Duration.between(LocalDateTime.now(), targetTime);

        ctx.sleep("waitUntilAprilFirst", delay);

        ctx.awaitExecute("sendReminder",
                         payload("to", email).getValues(),
                         ReminderService::send);
        // end::scheduled-delayed-sleep[]
    }
}
