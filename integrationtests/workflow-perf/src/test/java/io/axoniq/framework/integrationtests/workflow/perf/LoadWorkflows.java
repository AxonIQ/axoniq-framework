/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.integrationtests.workflow.perf;

import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import io.axoniq.framework.workflow.dsl.api.retry.BackoffStrategy;
import io.axoniq.framework.workflow.dsl.api.retry.RetryPolicy;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.payloadProperty;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;

/**
 * Five production-shaped workflows for the hibernation load test. Each one parks on a {@code waitForEvent} or a
 * {@code sleep} with a timeout of days right after it starts, so a hibernating engine can unload it. The execute steps
 * record when they run in {@link Probe}, which is how the test measures wake latency and deadline accuracy.
 *
 * @author Stefan Dragisic
 */
final class LoadWorkflows {

    private LoadWorkflows() {
    }

    // ---------------------------------------------------------------------------------------------------------
    // Events
    // ---------------------------------------------------------------------------------------------------------

    record OrderLine(String sku, int quantity, BigDecimal unitPrice) {

    }

    record OrderPlaced(String orderId, String customerId, List<OrderLine> lines, BigDecimal total, String currency,
                       Instant placedAt) {

    }

    record PaymentReceived(String orderId, String paymentId, BigDecimal amount, String currency, String method) {

    }

    record OrderShipped(String orderId, String carrier, String trackingNumber) {

    }

    record SubscriptionActivated(String subscriptionId, String customerId, String plan, long amountCents,
                                 String currency, Instant renewAt) {

    }

    record KycDocumentsSubmitted(String caseId, String customerId, String country, List<String> documentTypes) {

    }

    record KycDecisionMade(String caseId, String decision, String reviewer) {

    }

    record LoanApplicationSubmitted(String applicationId, String customerId, BigDecimal amount, int termMonths,
                                    String purpose) {

    }

    record PayslipReceived(String applicationId, String documentId, BigDecimal monthlyNetIncome) {

    }

    record IdentityVerified(String applicationId, String documentId, String verifier) {

    }

    record BankStatementReceived(String applicationId, String documentId, int months) {

    }

    record ParcelHandedToCarrier(String shipmentId, String orderId, String carrier, String trackingNumber) {

    }

    record ParcelDelivered(String shipmentId, Instant deliveredAt, String signedBy) {

    }

    record ParcelLost(String shipmentId, String reason) {

    }

    // ---------------------------------------------------------------------------------------------------------
    // Probe
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Wall-clock marks set by the test and by the execute steps. Execute steps run once per step, never on replay, so
     * each mark is the moment the step really ran.
     */
    static final class Probe {

        /** orderId to the nanoTime the test published its PaymentReceived. */
        static final Map<String, Long> paymentPublished = new ConcurrentHashMap<>();
        /** orderId to the nanoTime reserveStock started its first attempt. */
        static final Map<String, Long> stockReserved = new ConcurrentHashMap<>();
        /** subscriptionId to the epoch millis chargeCard started its first attempt. */
        static final Map<String, Long> charged = new ConcurrentHashMap<>();
        static final AtomicInteger chargeAttempts = new AtomicInteger();
        /** Subscriptions that already had a charge attempt; a declined first attempt is retried. */
        static final Set<String> chargeRetries = ConcurrentHashMap.newKeySet();
        static final AtomicInteger escalations = new AtomicInteger();
        static final AtomicInteger cancelledOrders = new AtomicInteger();

        private Probe() {
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Workflows
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Waits up to 3 days for the payment, reserves stock with retries, then waits up to 7 days for the shipment.
     */
    static final class OrderFulfillment {

        void execute(SimpleWorkflowContext ctx) {
            var orderId = String.valueOf(ctx.workflowPayload().get("orderId"));
            var payment = ctx.waitForEvent("awaitPayment", PaymentReceived.class,
                                           associate(payloadProperty("orderId"), equalsTo(orderId)),
                                           step -> step.timeout(Duration.ofDays(3)));
            payment.await();
            if (!payment.success()) {
                ctx.awaitExecute("cancelOrder", Map.of("orderId", orderId), (pc, in) -> {
                    Probe.cancelledOrders.incrementAndGet();
                    return Map.of("cancelled", true);
                });
                return;
            }
            ctx.awaitExecute("reserveStock", Map.of("orderId", orderId), (pc, in) -> {
                                 Probe.stockReserved.putIfAbsent(orderId, System.nanoTime());
                                 return Map.of("reservationId", "res-" + orderId);
                             },
                             step -> step.retryPolicy(RetryPolicy.maxRetries(3).withBackoff(
                                     BackoffStrategy.exponential(Duration.ofMillis(200), Duration.ofSeconds(5)))));
            ctx.awaitEvent("awaitShipment", OrderShipped.class,
                           associate(payloadProperty("orderId"), equalsTo(orderId)),
                           step -> step.timeout(Duration.ofDays(7)));
        }
    }

    /**
     * Sends a welcome, sleeps until the renewal date, then charges the card with retries. Every 10th subscription
     * fails its first charge attempt.
     */
    static final class SubscriptionRenewal {

        void execute(SimpleWorkflowContext ctx) {
            var subscriptionId = String.valueOf(ctx.workflowPayload().get("subscriptionId"));
            var renewAt = Instant.parse(String.valueOf(ctx.workflowPayload().get("renewAt")));
            ctx.awaitExecute("sendWelcome", Map.of("subscriptionId", subscriptionId),
                             (pc, in) -> Map.of("welcomeSent", true));
            // The sleep records its absolute deadline when it starts, so a replay waits for the same instant.
            var untilRenewal = Duration.between(Instant.now(), renewAt);
            ctx.awaitSleep("untilRenewal", untilRenewal.isNegative() ? Duration.ZERO : untilRenewal);
            ctx.awaitExecute("chargeCard", Map.of("subscriptionId", subscriptionId), (pc, in) -> {
                                 Probe.charged.putIfAbsent(subscriptionId, System.currentTimeMillis());
                                 Probe.chargeAttempts.incrementAndGet();
                                 if (Probe.chargeRetries.add(subscriptionId)
                                         && Math.floorMod(subscriptionId.hashCode(), 10) == 0) {
                                     throw new IllegalStateException("Card declined, try again");
                                 }
                                 return Map.of("chargeId", "ch-" + subscriptionId);
                             },
                             step -> step.retryPolicy(RetryPolicy.maxRetries(4).withBackoff(
                                     BackoffStrategy.exponential(Duration.ofMillis(500), Duration.ofSeconds(10)))));
        }
    }

    /**
     * Waits up to 2 days for a human decision, escalates to compliance on timeout, then waits up to 5 more days.
     */
    static final class KycReview {

        void execute(SimpleWorkflowContext ctx) {
            var caseId = String.valueOf(ctx.workflowPayload().get("caseId"));
            var decision = ctx.waitForEvent("awaitDecision", KycDecisionMade.class,
                                            associate(payloadProperty("caseId"), equalsTo(caseId)),
                                            step -> step.timeout(Duration.ofDays(2)));
            decision.await();
            if (decision.success()) {
                return;
            }
            ctx.awaitExecute("escalateToCompliance", Map.of("caseId", caseId), (pc, in) -> {
                Probe.escalations.incrementAndGet();
                return Map.of("escalated", true);
            });
            ctx.awaitEvent("awaitComplianceDecision", KycDecisionMade.class,
                           associate(payloadProperty("caseId"), equalsTo(caseId)),
                           step -> step.timeout(Duration.ofDays(5)));
        }
    }

    /**
     * Waits for 3 documents in parallel, each with a 14-day deadline, then scores the application.
     */
    static final class LoanApplication {

        void execute(SimpleWorkflowContext ctx) {
            var applicationId = String.valueOf(ctx.workflowPayload().get("applicationId"));
            var byApplication = associate(payloadProperty("applicationId"), equalsTo(applicationId));
            var payslip = ctx.waitForEvent("awaitPayslip", PayslipReceived.class, byApplication,
                                           step -> step.timeout(Duration.ofDays(14)));
            var identity = ctx.waitForEvent("awaitIdentity", IdentityVerified.class, byApplication,
                                            step -> step.timeout(Duration.ofDays(14)));
            var statement = ctx.waitForEvent("awaitBankStatement", BankStatementReceived.class, byApplication,
                                             step -> step.timeout(Duration.ofDays(14)));
            ctx.allMatch(WorkflowStepResult::success, payslip, identity, statement).await();
            ctx.awaitExecute("scoreApplication", Map.of("applicationId", applicationId),
                             (pc, in) -> Map.of("score", 700));
        }
    }

    /**
     * Waits up to 10 days for the parcel to be delivered or reported lost, whichever comes first.
     */
    static final class ShipmentTracking {

        void execute(SimpleWorkflowContext ctx) {
            var shipmentId = String.valueOf(ctx.workflowPayload().get("shipmentId"));
            var byShipment = associate(payloadProperty("shipmentId"), equalsTo(shipmentId));
            var delivered = ctx.waitForEvent("awaitDelivered", ParcelDelivered.class, byShipment,
                                             step -> step.timeout(Duration.ofDays(10)));
            var lost = ctx.waitForEvent("awaitLost", ParcelLost.class, byShipment,
                                        step -> step.timeout(Duration.ofDays(10)));
            ctx.anyMatch(WorkflowStepResult::isCompleted, delivered, lost).await();
            ctx.awaitExecute("notifyCustomer", Map.of("shipmentId", shipmentId),
                             (pc, in) -> Map.of("notified", true));
        }
    }
}
