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
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.axoniq.workflow.dsl.api.AssociationsUtils.associate;
import static io.axoniq.workflow.dsl.simple.SimpleWorkflowContext.equalsTo;
import static io.axoniq.workflow.runtime.api.payload.PayloadReducer.COMBINE_GLOBAL_AND_LOCAL;
import static io.axoniq.workflow.runtime.api.payload.PayloadReducer.LOCAL_ONLY;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for COMBINE_LOCAL_AND_CONTEXT result reducer.
 */
class CombineResultIntegrationTest
        extends AbstractDeclarativeTestBase<CombineResultIntegrationTest.CombineWorkflowContext> {

    public CombineResultIntegrationTest() {
        super(CombineWorkflowContext.class, c -> new CombineWorkflowContextFactory());
    }

    @Override
    protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<CombineWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<CombineWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .declarative(c -> new CombineWorkflow()::execute)
                .workflowName("CombineWorkflow")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class,
                                             associate(payloadProperty("status"), equalsTo("combine"))))
                .notCustomized();
    }

    @Test
    void shouldWriteStepResultToWorkflowPayloadWhenUsingCombine() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("wf-combine", "test@axoniq.io", "combine"))
        ));

        delayedPublisher.start();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });

        var history = workflowHistoryRepository.findAll().iterator().next();
        assertThat(history.state().workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);

        var payload = history.state().payload();
        // The step "combineStep" should have written its result to the workflow payload because we used COMBINE
        assertThat(payload.get("stepResult")).isEqualTo("combinedValue");

        // The initial payload from RegistrationReceivedEvent should also be present
        assertThat(payload.get("email")).isEqualTo("test@axoniq.io");
        assertThat(payload.get("status")).isEqualTo("combine");
    }

    public static class CombineWorkflow {

        public void execute(CombineWorkflowContext ctx) {
            // This call uses the overridden execute method which uses COMBINE_LOCAL_AND_CONTEXT for result reducer
            var result = ctx.execute("combineStep", Map.of(), (c, p) -> Map.of("stepResult", "combinedValue"));
            result.await();
        }
    }

    public static class CombineWorkflowContext extends SimpleWorkflowContext {

        public CombineWorkflowContext(String workflowId, Map<String, Object> payload,
                                      ProcessingContext processingContext,
                                      WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }

        @Override
        @Nonnull
        public WorkflowStepResult execute(
                @Nonnull String stepName,
                @Nonnull Map<String, Object> payload,
                @Nonnull PayloadProcessor action
        ) {
            return super.execute(stepName, payload, action, LOCAL_ONLY,
                                 COMBINE_GLOBAL_AND_LOCAL, Duration.ofMinutes(5), defaults());
        }

        @Override
        @Nonnull
        public WorkflowStepResult execute(
                @Nonnull String stepName,
                @Nonnull Map<String, Object> payload,
                @Nonnull PayloadProcessor action,
                @Nonnull Duration duration,
                @Nonnull EventNameCustomizer eventNameCustomizer
        ) {
            // Force COMBINE_LOCAL_AND_CONTEXT as result reducer
            // Note: SimpleWorkflowContext.execute delegates to the internal delegate which is a WorkflowContextDelegation
            return super.execute(stepName, payload, action, LOCAL_ONLY,
                                 COMBINE_GLOBAL_AND_LOCAL, duration, eventNameCustomizer);
        }
    }

    public static class CombineWorkflowContextFactory implements WorkflowContextFactory<CombineWorkflowContext> {

        @Override
        @Nonnull
        public CombineWorkflowContext createContext(@Nonnull Map<String, Object> payload, @Nonnull String workflowId,
                                                    @Nonnull ProcessingContext processingContext,
                                                    @Nonnull WorkflowConfiguration<?> workflowConfiguration) {
            return new CombineWorkflowContext(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
