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
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.workflow.runtime.association.Associations.associate;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: typed {@code awaitEvent} must surface a clean {@link StepTimedOutException} when the awaited event
 * never arrives within the configured timeout, rather than a Jackson {@code ConversionException} caused by running the
 * converter on the timeout step's bare {@link java.time.Instant} payload.
 *
 * @author Stefan Dragisic
 */
class AwaitEventTimeoutWorkflowTest extends AbstractWorkflowTestBase<SimpleWorkflowContext> {

    private AwaitEventTimeoutWorkflow workflow;

    public AwaitEventTimeoutWorkflowTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowConfigurer> configure() {
        workflow = new AwaitEventTimeoutWorkflow();
        return super.configure();
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> workflow);
    }

    @Test
    void awaitEventMustSurfaceTimeoutAsStepTimedOutException() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent("payer-1", "p@test.com", "vip"))
                // PaymentReceivedEvent is never published — awaitEvent must hit the timeout.
        ));
        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus().isTerminal());

        var state = testDriver.testingState().state();
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

    public static class AwaitEventTimeoutWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(AwaitEventTimeoutWorkflow.class);

        private static final Duration AWAIT_TIMEOUT = Duration.ofMillis(200);

        private final AtomicReference<Throwable> caughtException = new AtomicReference<>();

        Throwable caughtException() {
            return caughtException.get();
        }

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.awaitTimeout",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(SimpleWorkflowContext ctx) {
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
                ctx.fail(t);
            }
        }
    }

    @Event(namespace = "my.custom", name = "PaymentReceived")
    public record PaymentReceivedEvent(String id, long amount) {

    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
