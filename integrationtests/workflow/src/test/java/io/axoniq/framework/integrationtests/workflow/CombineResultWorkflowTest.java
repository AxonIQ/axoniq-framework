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
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;

/**
 * Integration test for COMBINE_LOCAL_AND_CONTEXT result reducer.
 */
class CombineResultWorkflowTest
        extends AbstractWorkflowIntegrationTestBase<CombineResultWorkflowTest.CombineWorkflowContext> {

    public CombineResultWorkflowTest() {
        super(CombineWorkflowContext.class, c -> new CombineWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<CombineWorkflowContext>, FinalizedPhase<CombineWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new CombineWorkflow());
    }

    @Test
    void shouldWriteStepResultToWorkflowPayloadWhenUsingCombine() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("wf-combine", "test@axoniq.io", "combine"))
        ));

        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus() == WorkflowStatus.COMPLETED);
        testDriver.testingState().payloadMatches(payload -> payload.entrySet().containsAll(Map.of(
                "stepResult", "combinedValue",
                "email", "test@axoniq.io",
                "status", "combine"
        ).entrySet()));
    }

    public static class CombineWorkflow {

        @Workflow(
                workflowName = "CombineWorkflow",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class,
                startOnConditions = {"payload:status=combine"}
        )
        public void execute(CombineWorkflowContext ctx) {
            var result = ctx.execute(
                    "combineStep",
                    Map.of(),
                    (c, p) -> Map.of("stepResult", "combinedValue"),
                    step -> step.resultPayloadReducer(
                                        CombineGlobalAndLocalPayloadReducer.INSTANCE
                                )
                                .timeout(Duration.ofMinutes(5))
            );
            result.await();
        }
    }

    public static class CombineWorkflowContext extends SimpleWorkflowContext {

        public CombineWorkflowContext(String workflowId, Map<String, @Nullable Object> payload,
                                      ProcessingContext processingContext,
                                      WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }

    public static class CombineWorkflowContextFactory implements WorkflowContextFactory<CombineWorkflowContext> {

        @Override
        public CombineWorkflowContext createContext(Map<String, @Nullable Object> payload, String workflowId,
                                                    ProcessingContext processingContext,
                                                    WorkflowConfiguration<?> workflowConfiguration) {
            return new CombineWorkflowContext(workflowId, payload, processingContext, workflowConfiguration);
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
