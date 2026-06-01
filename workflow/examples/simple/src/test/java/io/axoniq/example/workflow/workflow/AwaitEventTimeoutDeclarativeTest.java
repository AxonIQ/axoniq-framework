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
 * Regression test: typed {@code awaitEvent} must surface a clean {@link StepTimedOutException} when
 * the awaited event never arrives within the configured timeout, rather than a Jackson
 * {@code ConversionException} caused by running the converter on the timeout step's bare
 * {@link java.time.Instant} payload.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class AwaitEventTimeoutDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    private final AwaitEventTimeoutWorkflow workflow = new AwaitEventTimeoutWorkflow();

    public AwaitEventTimeoutDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("Await event timeout workflow")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.awaitTimeout").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "await-" + id))
                );
    }

    @Test
    void awaitEventMustSurfaceTimeoutAsStepTimedOutException() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent("payer-1", "p@test.com", "vip"))
                // PaymentReceivedEvent is never published — awaitEvent must hit the timeout.
        ));
        delayedPublisher.start();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });

        var state = workflowHistoryRepository.findAll().iterator().next().state();
        assertThat(state.getStep("waitForPayment").status())
                .as("step that never received the awaited event must be TIMED_OUT")
                .isEqualTo(StepStatus.TIMED_OUT);

        Throwable surfaced = workflow.caughtException();
        assertThat(surfaced)
                .as("awaitEvent must throw a StepTimedOutException on timeout, not a Jackson ConversionException")
                .isInstanceOf(StepTimedOutException.class);
        assertThat(surfaced.getMessage())
                .contains("waitForPayment")
                .contains("PaymentReceivedEvent");
    }
}
