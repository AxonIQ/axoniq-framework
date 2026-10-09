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

package workflows.steporchestration;

import io.axoniq.framework.workflow.dsl.api.CombinatorWorkflowStepResult;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.payloadProperty;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;

public class StepOrchestrationExamples {

    private static final Logger logger = LoggerFactory.getLogger(StepOrchestrationExamples.class);

    public void nonBlockingExecute(SimpleWorkflowContext context) {
        // tag::non-blocking-execute[]
        var shipping = context.execute(
                "shipOrder",
                context.workflowPayload(),
                ShippingService::shipOrder,
                step -> step.timeout(Duration.ofMinutes(5))
        );

        var notification = context.execute(
                "notifyCustomer",
                context.workflowPayload(),
                NotificationService::sendConfirmation,
                step -> step.timeout(Duration.ofSeconds(30))
        );
        // end::non-blocking-execute[]
    }

    public void mixExecuteAndWaitForEvent(SimpleWorkflowContext context) {
        // tag::mix-execute-waitforevent[]
        var approval = context.waitForEvent(
                "awaitApproval",
                ManagerApproved.class,
                associate(payloadProperty("orderId"), equalsTo(context.workflowId())),
                step -> step.timeout(Duration.ofMinutes(30))
        );

        var backgroundCheck = context.execute(
                "runBackgroundCheck",
                context.workflowPayload(),
                ComplianceService::check,
                step -> step.timeout(Duration.ofMinutes(10))
        );
        // end::mix-execute-waitforevent[]
    }

    public void anyMatchRaceEvents(SimpleWorkflowContext context) {
        // tag::anymatch-race-events[]
        var accepted = context.waitForEvent(
                "paymentAccepted",
                CardPaymentAccepted.class,
                associate(payloadProperty("orderId"), equalsTo(context.workflowId())),
                step -> step.timeout(Duration.ofMinutes(10))
        );

        var declined = context.waitForEvent(
                "paymentDeclined",
                CardPaymentDeclined.class,
                associate(payloadProperty("orderId"), equalsTo(context.workflowId())),
                step -> step.timeout(Duration.ofMinutes(10))
        );

        var result = context.anyMatch(
                WorkflowStepResult::isCompleted,    // <1>
                accepted, declined
        );

        result.await(); // <2>

        var winner = result.matched().getFirst();   // <3>
        logger.info("Payment outcome: {}", winner.getStepName());

        // Cancel the wait for the other event
        for (var remaining : result.unmatched()) {  // <4>
            remaining.cancel("Outcome already determined");
        }

        if (winner == declined) {
            context.fail(new RuntimeException("Payment was declined"));
        }
        // end::anymatch-race-events[]
    }

    public void anyMatchRaceServices(SimpleWorkflowContext context) {
        // tag::anymatch-race-services[]
        var dhlQuote = context.execute(
                "getDhlQuote",
                context.workflowPayload(),
                DhlService::getQuote,
                step -> step.timeout(Duration.ofSeconds(10))
        );

        var fedexQuote = context.execute(
                "getFedexQuote",
                context.workflowPayload(),
                FedexService::getQuote,
                step -> step.timeout(Duration.ofSeconds(10))
        );

        var race = context.anyMatch(
                WorkflowStepResult::isCompleted,
                dhlQuote, fedexQuote
        );
        race.await();

        var winner = race.matched().getFirst();
        logger.info("Using {}, quoted first", winner.getStepName());

        // Cancel the slower carrier
        for (var loser : race.unmatched()) {
            loser.cancel("Went with " + winner.getStepName());
        }
        // end::anymatch-race-services[]
    }

    public void allMatchBasic(SimpleWorkflowContext context) {
        // tag::allmatch-basic[]
        // Ship and notify in parallel
        var shipping = context.execute(
                "shipOrder",
                context.workflowPayload(),
                ShippingService::shipOrder,
                step -> step.timeout(Duration.ofMinutes(5))
        );

        var notification = context.execute(
                "notifyCustomer",
                context.workflowPayload(),
                NotificationService::sendConfirmation,
                step -> step.timeout(Duration.ofSeconds(30))
        );

        var guard = context.allMatch(
                WorkflowStepResult::success,    // <1>
                shipping, notification
        );

        if (guard.success()) {  // <2>
            logger.info("All steps completed successfully!");
        } else {
            // Something failed, inspect what went wrong
            for (var failed : guard.unmatched()) {  // <3>
                logger.warn("Step {} did not succeed", failed.getStepName());
            }
            context.fail(new RuntimeException("Not all steps completed successfully"));
        }
        // end::allmatch-basic[]
    }

    public void noneMatchBasic(SimpleWorkflowContext context) {
        // tag::nonematch-basic[]
        var stepA = context.execute(
                "processA",
                Map.of(),
                ServiceA::process,
                step -> step.timeout(Duration.ofMinutes(5))
        );
        var stepB = context.execute(
                "processB",
                Map.of(),
                ServiceB::process,
                step -> step.timeout(Duration.ofMinutes(5))
        );
        var stepC = context.execute(
                "processC",
                Map.of(),
                ServiceC::process,
                step -> step.timeout(Duration.ofMinutes(5))
        );

        var check = context.noneMatch(
                WorkflowStepResult::failure,          // <1>
                stepA, stepB, stepC
        );
        if (check.success()) {
            logger.info("No failures, all clear!");
        }
        // end::nonematch-basic[]
    }

    public void noneMatchCancelRemaining(SimpleWorkflowContext context,
                                         WorkflowStepResult stepA,
                                         WorkflowStepResult stepB,
                                         WorkflowStepResult stepC) {
        // tag::nonematch-cancel-remaining[]
        var check = context.noneMatch(
                WorkflowStepResult::failure,
                stepA, stepB, stepC
        );
        check.await();

        if (!check.success()) {
            var failedStep = check.matched().getFirst();    // <1>
            logger.warn("Step {} failed", failedStep.getStepName());

            // Cancel all steps that are still running
            for (var remaining : check.unmatched()) {   // <2>
                remaining.cancel("Cancelled due to failure of " + failedStep.getStepName());
            }

            context.fail(new RuntimeException("Step " + failedStep.getStepName() + " failed"));
        }
        // end::nonematch-cancel-remaining[]
    }

    public void noneMatchCompensate(SimpleWorkflowContext context) {
        // tag::nonematch-compensate[]
        var reserveStock = context.execute(
                "reserveStock",
                context.workflowPayload(),
                InventoryService::reserveStock,
                step -> step.timeout(Duration.ofMinutes(1))
        );
        var reserveShipping = context.execute(
                "reserveShipping",
                context.workflowPayload(),
                ShippingService::reserveSlot,
                step -> step.timeout(Duration.ofMinutes(1))
        );
        var chargePayment = context.execute(
                "chargePayment",
                context.workflowPayload(),
                PaymentService::charge,
                step -> step.timeout(Duration.ofSeconds(30))
        );

        var check = context.noneMatch(
                WorkflowStepResult::failure,
                reserveStock, reserveShipping, chargePayment
        );
        check.await();

        if (!check.success()) {
            var failedStep = check.matched().getFirst();

            // 1. Cancel steps that are still running
            for (var remaining : check.unmatched()) {
                if (!remaining.isCompleted()) {
                    remaining.cancel("Rolling back due to " + failedStep.getStepName());
                }
            }

            // 2. Compensate steps that already completed successfully
            for (var completed : check.unmatched()) {                   // <1>
                if (completed.success()) {
                    context.awaitExecute(
                            "rollback-" + completed.getStepName(),
                            context.workflowPayload(),
                            CompensationService::rollback
                    );
                }
            }

            context.fail(new RuntimeException(
                    "Order failed at " + failedStep.getStepName()
                            + ": " + failedStep.error().map(Throwable::getMessage)
                                               .orElse("unknown")
            ));
        }
        // end::nonematch-compensate[]
    }

    public void generalPattern(CombinatorWorkflowStepResult result) {
        // tag::general-pattern[]
        result.await();

        if (!result.success()) {
            // 1. Who caused the problem?
            var trigger = result.matched().getFirst();    // for noneMatch: the violator
            // for anyMatch: the winner

            // 2. Clean up the rest
            for (var step : result.unmatched()) {
                if (!step.isCompleted()) {
                    step.cancel("No longer needed, " + trigger.getStepName()
                                        + " already resolved");  // still running -> cancel
                } else if (step.success()) {
                    // already completed -> compensate if needed
                }
            }
        }
        // end::general-pattern[]
    }
}
