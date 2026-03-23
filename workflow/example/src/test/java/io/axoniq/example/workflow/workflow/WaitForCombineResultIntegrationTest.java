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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.PayloadReducer;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.execution.EventConditions;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.dsl.simple.SimpleWorkflowContext.equalsTo;
import static io.axoniq.workflow.runtime.api.PayloadReducer.COMBINE_GLOBAL_AND_LOCAL;
import static io.axoniq.workflow.runtime.engine.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.workflow.runtime.engine.util.AssociationsUtils.associate;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for COMBINE result reducer for WaitFor primitive.
 */
class WaitForCombineResultIntegrationTest extends AbstractDeclarativeTestBase<WaitForCombineResultIntegrationTest.WaitForCombineWorkflowContext> {

    public WaitForCombineResultIntegrationTest() {
        super(WaitForCombineWorkflowContext.class, c -> new WaitForCombineWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<WaitForCombineWorkflowContext>> getDeclaredDefinitions() {
        return d -> d
                .declarative(c -> new WaitForCombineWorkflow()::execute)
                .workflowName("WaitForCombineWorkflow")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class,
                                             associate(payloadProperty("status"), equalsTo("waitForCombine"))))
                .notCustomized();
    }

    @Test
    void shouldWriteEventPayloadToWorkflowPayloadWhenUsingCombine() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("wf-wait-combine", "test@axoniq.io", "waitForCombine")),
                ofMillis(1000, new RegistrationReceivedEvent("wf-wait-combine", "other@axoniq.io", "arrived"))
        ));

        delayedPublisher.start();

        await().untilAsserted(() -> assertThat(workflowEngine.workflowExecutions()).isNotEmpty());

        workflowEngine.runWorkflows(false);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });

        var execution = workflowEngine.workflowExecutions().iterator().next();
        assertThat(execution.state().workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);

        var payload = execution.workflowContext().workflowPayload();
        // The event "arrived" should have its payload merged into the workflow context
        assertThat(payload.get("email")).isEqualTo("other@axoniq.io");
        assertThat(payload.get("status")).isEqualTo("arrived");
    }

    public static class WaitForCombineWorkflow {
        public void execute(SimpleWorkflowContext ctx) {
            // Using the new overload of awaitEvent with COMBINE
            ctx.awaitEvent("waitStep", RegistrationReceivedEvent.class,
                           associate(payloadProperty("status"), equalsTo("arrived")),
                           COMBINE_GLOBAL_AND_LOCAL,
                           Duration.ofSeconds(5));
        }
    }

    public static class WaitForCombineWorkflowContext extends SimpleWorkflowContext {
        public WaitForCombineWorkflowContext(String workflowId, Map<String, Object> payload, ProcessingContext processingContext, WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }

        @Override
        @Nonnull
        public WorkflowStepResult waitFor(
                @Nonnull String stepName,
                @Nonnull EventCondition eventCondition,
                @Nonnull PayloadReducer resultPayloadReducer,
                @Nonnull Duration timeout,
                @Nonnull EventNameCustomizer eventNameCustomizer
        ) {
            return super.waitFor(stepName, eventCondition, resultPayloadReducer, timeout, eventNameCustomizer);
        }
    }

    public static class WaitForCombineWorkflowContextFactory implements WorkflowContextFactory<WaitForCombineWorkflowContext> {
        @Override
        @Nonnull
        public WaitForCombineWorkflowContext createContext(@Nonnull Map<String, Object> payload, @Nonnull String workflowId, @Nonnull ProcessingContext processingContext, @Nonnull WorkflowConfiguration<?> workflowConfiguration) {
            return new WaitForCombineWorkflowContext(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
