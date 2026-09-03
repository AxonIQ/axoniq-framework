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

import io.axoniq.workflow.configuration.WorkflowConfigurer;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.test.AbstractWorkflowTestBase;
import io.axoniq.workflow.runtime.test.utils.PrettyPrintingRecordingEventStore;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.workflow.runtime.association.Associations.associate;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that a running step can be cancelled from a thread other than the workflow's own control thread, and that
 * the workflow can catch the resulting {@link StepCancellationException} and continue with further steps (e.g.
 * compensation) to normal completion.
 *
 * @author Stefan Dragisic
 */
class ExternalStepCancellationWorkflowTest extends AbstractWorkflowTestBase<SimpleWorkflowContext> {

    private static final String WORKFLOW_ID = "external-cancel-1";

    public ExternalStepCancellationWorkflowTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowConfigurer> configure() {
        return super.configure();
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d.autodetected(c -> new ExternalStepCancellationWorkflow());
    }

    @Test
    void externallyCancelledStepCanBeCaughtAndCompensated() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent(WORKFLOW_ID, "cancel@test.com", "active"))
                // ApprovalEvent is never published — the step is cancelled externally instead.
        ));
        delayedPublisher.start();

        var executionRepository = configuration.getComponent(WorkflowExecutionRepository.class);

        // Wait until the workflow is up and blocked awaiting approval.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var execution = executionRepository.findById(WORKFLOW_ID);
            assertThat(execution).isPresent();
            var state = execution.get().state();
            assertThat(state.containsStep("awaitApproval")).isTrue();
            assertThat(state.getStep("awaitApproval").status()).isEqualTo(StepStatus.STARTED);
        });

        // Request cancellation from the test thread; the service queues the actual cancellation on the workflow control thread.
        workflowCancellationService.requestStepCancellation(
                WORKFLOW_ID, "awaitApproval", new StepCancellationException("cancelled externally")
        ).join();

        // The workflow must catch the cancellation, run the compensate step, and complete normally.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var history = workflowHistoryRepository.findAll().stream().findFirst();
            assertThat(history).isPresent();
            var state = history.get().state();
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(state.getStep("awaitApproval").status()).isEqualTo(StepStatus.CANCELLED);
            assertThat(state.getStep("compensate").status()).isEqualTo(StepStatus.COMPLETED);
        });

        var cancelledStepEvent = PrettyPrintingRecordingEventStore.lastInstance().recorded().stream()
                                                                  .filter(event -> MetadataUtils.getStepStatus(event.metadata())
                                                                                                .filter(StepStatus.CANCELLED::equals)
                                                                                                .isPresent())
                                                                  .findFirst();
        assertThat(cancelledStepEvent).isPresent();
        assertThat(cancelledStepEvent.orElseThrow().type().qualifiedName().toString())
                .isEqualTo("io.axoniq.dsl.externalcancel.AwaitApprovalCancelled");
    }

    public static class ExternalStepCancellationWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(ExternalStepCancellationWorkflow.class);

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.externalcancel",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            logger.info("ExternalStepCancellationWorkflow started for {}", ctx.workflowPayload());
            var id = String.valueOf(ctx.workflowPayload().get("id"));
            try {
                ctx.awaitEvent(
                        "awaitApproval",
                        ApprovalEvent.class,
                        associate(payloadProperty("id"), equalsTo(id)),
                        step -> step.timeout(Duration.ofMinutes(5))
                );
            } catch (StepCancellationException cancelled) {
                logger.info("Approval await cancelled, compensating: {}", cancelled.getMessage());
                ctx.awaitExecute("compensate", Map.of(), (c, p) -> Map.of("compensated", true));
            }
        }
    }

    @Event(namespace = "my.custom", name = "ApprovalReceived")
    public record ApprovalEvent(String id, boolean approved) {

    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
