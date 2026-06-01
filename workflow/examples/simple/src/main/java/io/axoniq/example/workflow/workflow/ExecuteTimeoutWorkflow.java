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

import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reproduction workflow that pins down what the {@code execute} primitive surfaces to user code on
 * timeout when the action busy-waits past the configured step timeout and the workflow then blocks
 * on {@code awaitExecute(...)}.
 * <p>
 * The {@code awaitExecute(...)} convenience throws on any non-success terminal status. After the
 * fix that mirrors PR&nbsp;#209, a {@code TIMED_OUT} step must surface as a dedicated
 * {@link io.axoniq.workflow.runtime.api.execution.state.StepTimedOutException} (instead of a
 * generic {@code StepFailedException} with {@code message=null} and {@code cause=null}).
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class ExecuteTimeoutWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(ExecuteTimeoutWorkflow.class);

    public static final Duration STEP_TIMEOUT = Duration.ofMillis(200);
    public static final long STEP_SLEEP_MILLIS = 1500L;

    private final AtomicReference<Throwable> caughtException = new AtomicReference<>();

    /**
     * Exposes the exception that escaped {@code awaitExecute} so the test can assert on its type and message.
     *
     * @return throwable caught around the {@code awaitExecute} call, or {@code null} if it returned normally.
     */
    public Throwable caughtException() {
        return caughtException.get();
    }

    public void execute(@Nonnull SimpleWorkflowContext ctx) {
        logger.info("ExecuteTimeoutWorkflow started for {}", ctx.workflowPayload());

        try {
            // awaitExecute on a step whose action busy-waits past STEP_TIMEOUT — the runtime
            // must time out the step. We want to observe what awaitExecute throws.
            Map<String, Object> result = ctx.awaitExecute(
                    "slowStep",
                    Map.of(),
                    (pc, payload) -> {
                        long deadline = System.currentTimeMillis() + STEP_SLEEP_MILLIS;
                        while (System.currentTimeMillis() < deadline) {
                            // busy-wait, ignore interrupts so the action cannot honor the timeout itself
                            Thread.interrupted();
                        }
                        return Map.of("slowStep", "done");
                    },
                    step -> step.timeout(STEP_TIMEOUT)
            );
            logger.info("Unexpectedly received result: {}", result);
        } catch (Throwable t) {
            logger.info("awaitExecute surfaced: {}", t.toString());
            caughtException.set(t);
            // Terminate the workflow with a clean failure so the test can observe a terminal state.
            ctx.fail(t);
        }
    }
}
