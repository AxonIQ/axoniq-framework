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

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;

/**
 * Integration test for COMBINE result reducer for WaitFor primitive.
 */
class WaitForCombineResultWorkflowTest
        extends AbstractWorkflowIntegrationTestBase<WaitForCombineResultWorkflowTest.WaitForCombineWorkflowContext> {

    public WaitForCombineResultWorkflowTest() {
        super(WaitForCombineWorkflowContext.class, c -> new WaitForCombineWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<WaitForCombineWorkflowContext>, FinalizedPhase<WaitForCombineWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new WaitForCombineWorkflow());
    }

    @Test
    void shouldWriteEventPayloadToWorkflowPayloadWhenUsingCombine() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("wf-wait-combine", "test@axoniq.io", "waitForCombine")),
                ofMillis(1000, new RegistrationReceivedEvent("wf-wait-combine", "other@axoniq.io", "arrived"))
        ));

        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus() == WorkflowStatus.COMPLETED);
        testDriver.testingState().payloadMatches(payload -> payload.entrySet().containsAll(Map.of(
                "email", "other@axoniq.io",
                "status", "arrived"
        ).entrySet()));
    }

    public static class WaitForCombineWorkflow {

        static final Logger logger = LoggerFactory.getLogger(WaitForCombineWorkflow.class);

        @Workflow(
                workflowName = "WaitForCombineWorkflow",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class,
                startOnConditions = {"payload:status=waitForCombine"}
        )
        public void execute(WaitForCombineWorkflowContext ctx) {
            var event = ctx.processingContext().component(EventConverter.class).convert(
                    ctx.awaitEvent(
                            "waitStep",
                            EventConditions.fromQualifiedName(
                                    ctx.processingContext()
                                       .component(MessageTypeResolver.class)
                                       .resolve(RegistrationReceivedEvent.class)
                                       .orElseThrow()
                                       .qualifiedName(),
                                    associate(payloadProperty("status"), equalsTo("arrived"))
                            ),
                            step -> step.resultPayloadReducer(
                                                CombineGlobalAndLocalPayloadReducer.INSTANCE
                                        )
                                        .timeout(Duration.ofSeconds(5))
                    ),
                    RegistrationReceivedEvent.class
            );
        }
    }

    public static class WaitForCombineWorkflowContext extends SimpleWorkflowContext {

        public WaitForCombineWorkflowContext(String workflowId, Map<String, @Nullable Object> payload,
                                             ProcessingContext processingContext,
                                             WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }

    public static class WaitForCombineWorkflowContextFactory
            implements WorkflowContextFactory<WaitForCombineWorkflowContext> {

        @Override
        public WaitForCombineWorkflowContext createContext(Map<String, @Nullable Object> payload,
                                                           String workflowId,
                                                           ProcessingContext processingContext,
                                                           WorkflowConfiguration<?> workflowConfiguration) {
            return new WaitForCombineWorkflowContext(workflowId, payload, processingContext, workflowConfiguration);
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
