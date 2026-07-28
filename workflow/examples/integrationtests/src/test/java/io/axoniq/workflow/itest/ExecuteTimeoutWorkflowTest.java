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
package io.axoniq.workflow.itest;

import io.axoniq.workflow.configuration.WorkflowConfigurer;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.api.execution.state.StepTimedOutException;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.test.AbstractWorkflowTestBase;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: {@code awaitExecute} must surface a dedicated {@link StepTimedOutException} when the executed action
 * runs past the configured step timeout, rather than a generic {@code StepFailedException} with {@code message=null}
 * and {@code cause=null}.
 * <p>
 * This is the {@code execute}-primitive counterpart to the typed-{@code awaitEvent} fix from PR&nbsp;#209. Both code
 * paths share the same root cause: {@code WorkflowStep#error()} is {@code null} for {@code TIMED_OUT}/{@code CANCELLED}
 * steps, so {@code result.error()} wraps {@code null} in a {@code StepFailedException} and {@code awaitExecute}'s
 * {@code result.error().orElseThrow()} surfaces that opaque wrapper instead of a meaningful dedicated type.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class ExecuteTimeoutWorkflowTest extends AbstractWorkflowTestBase<SimpleWorkflowContext> {

    private ExecuteTimeoutWorkflow workflow;

    public ExecuteTimeoutWorkflowTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowConfigurer> configure() {
        this.workflow = new ExecuteTimeoutWorkflow();
        return super.configure();
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> workflow);
    }

    @Test
    void awaitExecuteMustSurfaceTimeoutAsStepTimedOutException() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent("execute-timeout-1", "t@test.com", "n/a"))
        ));
        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus().isTerminal());

        var state = testDriver.testingState().state();
        assertThat(state.getStep("slowStep").status())
                .as("execute step whose action runs past the timeout must be TIMED_OUT")
                .isEqualTo(StepStatus.TIMED_OUT);

        Throwable surfaced = workflow.caughtException();
        assertThat(surfaced)
                .as("awaitExecute must throw a dedicated StepTimedOutException on timeout, "
                            + "mirroring the post-PR-#209 typed awaitEvent surface — "
                            + "current main throws %s instead",
                    surfaced == null ? "nothing" : surfaced.getClass().getName())
                .isInstanceOf(StepTimedOutException.class);
        assertThat(surfaced.getMessage())
                .as("the surfaced exception should name the step that timed out")
                .contains("slowStep");
    }

    public static class ExecuteTimeoutWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(ExecuteTimeoutWorkflow.class);

        private static final Duration STEP_TIMEOUT = Duration.ofMillis(200);
        private static final long STEP_SLEEP_MILLIS = 1500L;

        private final AtomicReference<Throwable> caughtException = new AtomicReference<>();

        Throwable caughtException() {
            return caughtException.get();
        }

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.executeTimeout",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(@Nonnull SimpleWorkflowContext ctx) {
            logger.info("ExecuteTimeoutWorkflow started for {}", ctx.workflowPayload());

            try {
                Map<String, Object> result = ctx.awaitExecute(
                        "slowStep",
                        Map.of(),
                        (pc, payload) -> {
                            long deadline = System.currentTimeMillis() + STEP_SLEEP_MILLIS;
                            while (System.currentTimeMillis() < deadline) {
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
                ctx.fail(t);
            }
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
