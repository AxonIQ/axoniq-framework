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

import io.axoniq.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.example.workflow.fixture.SleepUtils.sleepQuietly;

/**
 * Demonstrates {@link WorkflowContext#anyMatch} semantics: two steps are launched in parallel — a fast one (~500 ms)
 * and a slow one (5 min). The first to complete wins.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class AnyRaceWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(AnyRaceWorkflow.class);

    public void execute(@Nonnull BaseWorkflowContext ctx) {
        logger.info("anyMatch() workflow started for {}", ctx.workflowPayload());

        var fast = ctx.execute(
                "fastStep",
                Map.of(),
                (c, p) -> {
                    sleepQuietly(500);
                    return Map.of("winner", "fast");
                },
                step -> step.timeout(Duration.ofSeconds(10))
        );

        var slow = ctx.execute(
                "slowStep",
                Map.of(),
                (c, p) -> {
                    sleepQuietly(Duration.ofMinutes(5));
                    return Map.of("winner", "slow");
                },
                step -> step.timeout(Duration.ofMinutes(5))
        );

        // anyMatch: first to reach a terminal state wins
        var winner = ctx.anyMatch(WorkflowStepResult::isCompleted, fast, slow);

        winner.await();
        logger.info("Race won by: {}", winner.getStepName());
        logger.info("All finishers: {}",
                    winner.matched().stream().map(WorkflowStepResult::getStepName).toList());
    }
}
