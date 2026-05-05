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
import io.axoniq.workflow.dsl.api.AssociationsUtils;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.axoniq.workflow.dsl.api.AssociationsUtils.associate;
import static io.axoniq.workflow.dsl.simple.SimpleWorkflowContext.equalsTo;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static io.axoniq.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer.NAME_COMBINE_GLOBAL_AND_LOCAL;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for COMBINE result reducer for WaitFor primitive.
 */
class WaitForCombineResultIntegrationTest
        extends AbstractDeclarativeTestBase<WaitForCombineResultIntegrationTest.WaitForCombineWorkflowContext> {

    public WaitForCombineResultIntegrationTest() {
        super(WaitForCombineWorkflowContext.class, c -> new WaitForCombineWorkflowContextFactory());
    }

    @Override
    protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<WaitForCombineWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<WaitForCombineWorkflowContext>> getDeclaredDefinition() {
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

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });

        await().untilAsserted(() -> assertThat(workflowEngine.workflowExecutions()).isEmpty());

        var history = workflowHistoryRepository.findAll().iterator().next();
        assertThat(history.state().workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);

        var payload = history.state().payload();
        // The event "arrived" should have its payload merged into the workflow context
        assertThat(payload.get("email")).isEqualTo("other@axoniq.io");
        assertThat(payload.get("status")).isEqualTo("arrived");
    }

    public static class WaitForCombineWorkflow {

        static final Logger logger = LoggerFactory.getLogger(WaitForCombineWorkflow.class);

        public void execute(WaitForCombineWorkflowContext ctx) {
            // Using the new overload of awaitEvent with COMBINE
            var event = ctx.awaitEvent("waitStep",
                                       RegistrationReceivedEvent.class,
                                       associate(payloadProperty("status"), equalsTo("arrived")),
                                       Duration.ofSeconds(5)
            );
        }
    }

    public static class WaitForCombineWorkflowContext extends SimpleWorkflowContext {

        public WaitForCombineWorkflowContext(String workflowId, Map<String, Object> payload,
                                             ProcessingContext processingContext,
                                             WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }

        @Override
        public <T> T awaitEvent(String stepName, Class<T> eventType, AssociationsUtils associationsUtils,
                                Duration timeout) {
            return waitFor(PrimitiveCommands.blockingWait(
                    stepName,
                    EventConditions.fromQualifiedName(
                            super.processingContext()
                                 .component(MessageTypeResolver.class).resolve(eventType)
                                 .orElseThrow().qualifiedName(),
                            (e, pc) -> associationsUtils.build().test(e, pc)
                    ),
                    registry.get(NAME_COMBINE_GLOBAL_AND_LOCAL).orElseThrow(),
                    timeout,
                    TypeReference.fromType(eventType),
                    super.processingContext().component(EventConverter.class),
                    defaults()
            ));
        }
    }

    public static class WaitForCombineWorkflowContextFactory
            implements WorkflowContextFactory<WaitForCombineWorkflowContext> {

        @Override
        @Nonnull
        public WaitForCombineWorkflowContext createContext(@Nonnull Map<String, Object> payload,
                                                           @Nonnull String workflowId,
                                                           @Nonnull ProcessingContext processingContext,
                                                           @Nonnull WorkflowConfiguration<?> workflowConfiguration) {
            return new WaitForCombineWorkflowContext(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
