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

import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.state.StepTimedOutException;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Regression test: {@code awaitExecute} must surface a dedicated {@link StepTimedOutException} when
 * the executed action runs past the configured step timeout, rather than a generic
 * {@code StepFailedException} with {@code message=null} and {@code cause=null}.
 * <p>
 * This is the {@code execute}-primitive counterpart to the typed-{@code awaitEvent} fix from
 * PR&nbsp;#209. Both code paths share the same root cause: {@code WorkflowStep#error()} is
 * {@code null} for {@code TIMED_OUT}/{@code CANCELLED} steps, so {@code result.error()} wraps
 * {@code null} in a {@code StepFailedException} and {@code awaitExecute}'s
 * {@code result.error().orElseThrow()} surfaces that opaque wrapper instead of a meaningful
 * dedicated type.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class ExecuteTimeoutDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    private final ExecuteTimeoutWorkflow workflow = new ExecuteTimeoutWorkflow();

    public ExecuteTimeoutDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("Execute timeout workflow")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.executeTimeout").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "execute-timeout-" + id))
                );
    }

    @Test
    void awaitExecuteMustSurfaceTimeoutAsStepTimedOutException() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent("execute-timeout-1", "t@test.com", "n/a"))
        ));
        delayedPublisher.start();

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });

        var state = workflowHistoryRepository.findAll().iterator().next().state();
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
}
