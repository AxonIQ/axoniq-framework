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
 * Demonstrates {@link WorkflowContext#noneMatch} semantics:
 * two steps are launched in parallel — a fast one that fails (~500 ms) and a slow one (5 min).
 * The guard verifies that none of them fail; the fast failure short-circuits.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class NoneMatchGuardWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(NoneMatchGuardWorkflow.class);

    public void execute(@Nonnull SimpleWorkflowContext ctx) {
        logger.info("noneMatch() workflow started for {}", ctx.workflowPayload());

        var failingStep = ctx.execute("failingStep", Map.of(), (c, p) -> {
            sleepQuietly(500);
            throw new RuntimeException("step failed");
        }, Duration.ofSeconds(10), defaults());

        var slowStep = ctx.execute("slowStep", Map.of(), (c, p) -> {
            sleepQuietly(Duration.ofMinutes(5));
            return Map.of("result", "slow-done");
        }, Duration.ofMinutes(5), defaults());

        // noneMatch: guard that no step matches the failure predicate — short-circuits on first match
        var guard = ctx.noneMatch(WorkflowStepResult::failure, failingStep, slowStep);

        if (guard.failure()) {
            logger.info("Guard violated by: {}", guard.getStepName());
            logger.info("Violators (matched failure predicate): {}",
                    guard.matched().stream().map(WorkflowStepResult::getStepName).toList());
            logger.info("Clean steps (unmatched): {}",
                    guard.unmatched().stream().map(WorkflowStepResult::getStepName).toList());

        }
    }
}
