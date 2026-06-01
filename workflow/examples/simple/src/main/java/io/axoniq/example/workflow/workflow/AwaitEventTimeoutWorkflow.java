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
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.PaymentReceivedEvent;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.workflow.dsl.api.AssociationsUtils.associate;
import static io.axoniq.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * Reproduction workflow for the {@code awaitEvent} typed-helper crash on timeout.
 * <p>
 * The workflow calls {@code awaitEvent} with a short timeout and a typed event that is never
 * published. Before the fix, the helper unconditionally ran the converter against the timeout
 * step's bare {@link java.time.Instant} payload and surfaced a Jackson
 * {@code ConversionException} instead of a clean timeout signal.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class AwaitEventTimeoutWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(AwaitEventTimeoutWorkflow.class);

    public static final Duration AWAIT_TIMEOUT = Duration.ofMillis(200);

    private final AtomicReference<Throwable> caughtException = new AtomicReference<>();

    /**
     * Exposes the exception that escaped {@code awaitEvent} so the test can assert on its type and message.
     *
     * @return throwable caught around the {@code awaitEvent} call, or {@code null} if it returned normally.
     */
    public Throwable caughtException() {
        return caughtException.get();
    }

    public void execute(@Nonnull SimpleWorkflowContext ctx) {
        logger.info("AwaitEventTimeoutWorkflow started for {}", ctx.workflowPayload());

        var id = String.valueOf(ctx.workflowPayload().get("id"));
        try {
            var payment = ctx.awaitEvent(
                    "waitForPayment",
                    PaymentReceivedEvent.class,
                    associate(payloadProperty("id"), equalsTo(id)),
                    step -> step.timeout(AWAIT_TIMEOUT)
            );
            logger.info("Unexpectedly received payment: {}", payment);
        } catch (Throwable t) {
            logger.info("awaitEvent surfaced: {}", t.toString());
            caughtException.set(t);
            // Terminate the workflow with a clean failure so the test can observe a terminal state.
            ctx.fail(t);
        }
    }
}
