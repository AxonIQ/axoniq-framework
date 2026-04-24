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
package io.axoniq.example.workflow.workflow;

import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.example.workflow.fixture.SleepUtils.sleepQuietly;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;

/**
 * Fires an async step that runs longer than the workflow body, exercising the
 * "cancel running steps on terminal state" paths from issue #127. The terminal
 * branch is selected by the {@code status} payload attribute: {@code FAIL} calls
 * {@code ctx.fail(...)}, {@code CANCEL} calls {@code ctx.cancel(...)}; any other
 * value lets the workflow body return normally.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class CompleteWithRunningStepWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(CompleteWithRunningStepWorkflow.class);

    public void execute(@Nonnull SimpleWorkflowContext ctx) {
        logger.info("completeWithRunningStep workflow started for {}", ctx.workflowPayload());

        ctx.execute("background", Map.of(), (c, p) -> {
            sleepQuietly(Duration.ofMinutes(5));
            return Map.of("done", true);
        }, Duration.ofMinutes(5), defaults());

        var status = String.valueOf(ctx.workflowPayload().get("status"));
        switch (status) {
            case "FAIL" -> ctx.fail(new RuntimeException("boom"));
            case "CANCEL" -> ctx.cancel("user-initiated");
            default -> { /* let the workflow body return naturally */ }
        }
    }
}
