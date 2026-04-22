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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;

/**
 * Demonstrates {@link SimpleWorkflowContext#sleepAsync} — a non-blocking sleep composed
 * with an execute step via {@link WorkflowContext#anyMatch}.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class SleepAsyncWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(SleepAsyncWorkflow.class);

    public void execute(@Nonnull SimpleWorkflowContext ctx) {
        logger.info("sleepAsync workflow started for {}", ctx.workflowPayload());

        var delay = ctx.sleepAsync("cooldown", Duration.ofMinutes(5));

        var work = ctx.execute("doWork", Map.of(), (c, p) -> {
            return Map.of("done", true);
        }, Duration.ofSeconds(10), defaults());

        // proceed when either finishes
        ctx.anyMatch(WorkflowStepResult::isCompleted, delay, work).await();

        logger.info("sleepAsync workflow completed");
    }
}
