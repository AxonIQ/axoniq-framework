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

package workflows.understandingsteps;

import io.axoniq.framework.workflow.dsl.api.StepCancellationException;
import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.retry.BackoffStrategy;
import io.axoniq.framework.workflow.dsl.api.retry.RetryPolicy;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.payloadProperty;
import static io.axoniq.framework.workflow.dsl.api.Payload.payload;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;

public class UnderstandingStepsExamples {

    private static final Logger logger = LoggerFactory.getLogger(UnderstandingStepsExamples.class);

    public void stepResultInspect(SimpleWorkflowContext context) {
        // tag::step-result-inspect[]
        // non-blocking execute
        var shipping = context.execute(
                "shipOrder",
                context.workflowPayload(),
                ShippingService::shipOrder,
                step -> step.timeout(Duration.ofMinutes(5))
        );

        // non-blocking wait for event
        var approval = context.waitForEvent(
                "awaitApproval",
                ManagerApproved.class,
                associate(payloadProperty("orderId"),
                          equalsTo(context.workflowPayload().get("orderId"))),
                step -> step.timeout(Duration.ofMinutes(30))
        );

        // check and wait on either
        if (shipping.isCompleted()) {
            logger.info("Shipping already done!");
        }

        approval.await(); // block until the event arrives or times out

        if (approval.timeout()) {
            logger.warn("Approval timed out!");
        }
        // end::step-result-inspect[]
    }

    public void timeoutsExample(SimpleWorkflowContext context) {
        // tag::timeouts-example[]
        // Uses the default 5-second timeout
        context.awaitExecute(
                "quickCheck",
                Map.of(),
                (pc, input) -> Map.of("passed", QuickService.check())
        );

        // Explicit 30-second timeout for an action
        context.awaitExecute(
                "slowOperation",
                context.workflowPayload(),
                SlowService::process,
                step -> step.timeout(Duration.ofSeconds(30))
        );

        // Explicit 15-minute timeout for waiting on an external event
        context.awaitEvent(
                "awaitPayment",
                PaymentConfirmed.class,
                associate(payloadProperty("orderId"),
                          equalsTo(context.workflowPayload().get("orderId"))),
                step -> step.timeout(Duration.ofMinutes(15))
        );
        // end::timeouts-example[]
    }

    public void reducerDefault(
            SimpleWorkflowContext context,
            Object customerId
    ) {
        // tag::reducer-default[]
        // Default behavior: step only sees the local payload
        context.awaitExecute(
                "reserveStock",
                payload("customerId", customerId).getValues(), // <1>
                InventoryService::reserveStock
        );
        // end::reducer-default[]
    }

    public void reducerCombine(SimpleWorkflowContext context) {
        // tag::reducer-combine[]
        context.awaitExecute(
                "processOrder",
                payload("priority", "high").getValues(),
                OrderService::process,
                step -> step.parameterPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE)
                            .resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE)
        );
        // end::reducer-combine[]
    }

    public void cancelViaResult(SimpleWorkflowContext context) {
        // tag::cancel-via-result[]
        var approval = context.waitForEvent(
                "awaitApproval",
                ManagerApproved.class,
                associate(payloadProperty("orderId"),
                          equalsTo(context.workflowPayload().get("orderId"))),
                step -> step.timeout(Duration.ofMinutes(30))
        );

        // later, manager rejected, no longer need approval
        approval.cancel("Manager rejected the request"); // <1>
        // end::cancel-via-result[]
    }

    public void cancelViaContext(
            SimpleWorkflowContext context,
            Exception someException
    ) {
        // tag::cancel-via-context[]
        context.cancelStep("awaitApproval");    // default message: "Step cancelled"
        context.cancelStep("awaitApproval",     // custom reason
                           step -> step.cause(new StepCancellationException("Manager rejected")));
        context.cancelStep("awaitApproval",     // wrap an existing exception
                           step -> step.cause(someException));
        // end::cancel-via-context[]
    }

    public void externalStepCancellation(
            WorkflowManager workflowManager,
            String orderId
    ) {
        // tag::external-step-cancellation[]
        workflowManager.findOne(WorkflowStateQuery.byWorkflowId(orderId))
                       .requestStepCancellation("awaitApproval", null);
        // end::external-step-cancellation[]
    }

    public void crashInterruptedCatch(
            SimpleWorkflowContext context,
            Map<String, Object> payload
    ) {
        // tag::crash-interrupted-catch[]
        try {
            context.awaitExecute("chargePayment", payload, (pc, p) -> charge(p));
        } catch (StepFailedException e) {
            // crash-interrupted (StepIndeterminateException) or a genuine failure: compensate, then fail the workflow
            context.fail(e);
        }
        // end::crash-interrupted-catch[]
    }

    public void retriesExample(
            SimpleWorkflowContext context,
            String orderId,
            double amount,
            String shipmentId,
            String address
    ) {
        // tag::retries-example[]
        // Simple: retry up to 3 times (uses the default timeout and event names)
        context.awaitExecute(
                "reserveInventory",
                payload("orderId", orderId).getValues(),
                InventoryService::reserve,
                step -> step.retryPolicy(RetryPolicy.maxRetries(3)) // <1>
        );

        // With fixed backoff: wait 500ms between retries
        context.awaitExecute(
                "chargePayment",
                payload("orderId", orderId, "amount", amount).getValues(),
                PaymentGateway::charge,
                step -> step.retryPolicy(
                        RetryPolicy.maxRetries(3)
                                   .withBackoff(BackoffStrategy.fixed(Duration.ofMillis(500)))
                )
        );

        // With exponential backoff (base 200ms, capped at 5s)
        context.awaitExecute(
                "notifyCarrier",
                payload("shipmentId", shipmentId).getValues(),
                CarrierService::notify,
                step -> step.retryPolicy(
                        RetryPolicy.maxRetries(5)
                                   .withBackoff(BackoffStrategy.exponential(
                                           Duration.ofMillis(200),
                                           Duration.ofSeconds(5)
                                   ))
                )
        );

        // With retry handler and conditional stop
        context.awaitExecute(
                "validateAddress",
                payload("address", address).getValues(),
                AddressService::validate,
                step -> step.retryPolicy(
                        RetryPolicy.maxRetries(5)
                                   .onRetry(retryCtx -> logger.warn(
                                           "Retry {}/{} for {}: {}",       // <2>
                                           retryCtx.attempt(),
                                           retryCtx.maxRetries(),
                                           retryCtx.stepName(),
                                           retryCtx.error().getMessage()
                                   ))
                                   .retryWhile(retryCtx -> isTransient(retryCtx.error())) // <3>
                )
        );
        // end::retries-example[]
    }

    private Map<String, Object> charge(Map<String, Object> payload) {
        return payload;
    }

    private boolean isTransient(Throwable error) {
        return true;
    }
}
