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

import static io.axoniq.example.workflow.fixture.SleepUtils.sleepQuietly;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;

/**
 * Demonstrates {@link WorkflowContext#allMatch} semantics:
 * two steps are launched in parallel — one succeeds (~500 ms) and one fails (~500 ms).
 * The guard verifies that all of them succeed; the failure short-circuits.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class AllMatchGuardWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(AllMatchGuardWorkflow.class);

    public void execute(@Nonnull SimpleWorkflowContext ctx) {
        logger.info("allMatch() workflow started for {}", ctx.workflowPayload());

        var successStep = ctx.execute("successStep", Map.of(), (c, p) -> {
            sleepQuietly(500);
            return Map.of("result", "ok");
        }, Duration.ofSeconds(10), defaults());

        var failingStep = ctx.execute("failingStep", Map.of(), (c, p) -> {
            sleepQuietly(500);
            throw new RuntimeException("step failed");
        }, Duration.ofSeconds(10), defaults());



        // allMatch: guard that all steps match the success predicate — short-circuits on first non-match
        var guard = ctx.allMatch(WorkflowStepResult::success, successStep, failingStep);

        if (guard.failure()) {
            logger.info("Guard violated — not all steps succeeded: {}", guard.getStepName());
            logger.info("Successful steps (matched): {}",
                    guard.matched().stream().map(WorkflowStepResult::getStepName).toList());
            logger.info("Violators (unmatched): {}",
                    guard.unmatched().stream().map(WorkflowStepResult::getStepName).toList());


        }
    }
}
