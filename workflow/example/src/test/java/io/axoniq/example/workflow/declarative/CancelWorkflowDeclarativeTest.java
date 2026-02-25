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
package io.axoniq.example.workflow.declarative;

import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.configuration.PrettyPrintingRecordingEventStore;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class CancelWorkflowDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public CancelWorkflowDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Consumer<WorkflowModule.WorkflowDefinitionPhase.DefinitionPhase<SimpleWorkflowContext>> getDeclaredDefinitions() {
        var workflow = new CancelWorkflow();
        return (d) -> d.declarative("Cancel workflow in Java")
                       .on(EventCondition.fromType(RegistrationReceivedEvent.class))
                       .workflowDefinition(c -> workflow::execute)
                       .eventNameCustomizer(c -> () -> namespace("io.axoniq.dsl.cancel").workflowBaseName("Workflow"))
                       .workflowIdProvider(c -> (trigger) -> Optional.of("cancel-" + trigger.get("id").toString()))
                       .notCustomized();
    }

    @Test
    void workflowIsCancelled() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-789", "cancel@test.com"))
        ));

        delayedPublisher.start();

        await().untilAsserted(() -> {
            assertThat(workflowEngine.workflowInstances()).isNotEmpty();
        });

        workflowEngine.runWorkflows();

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowEngine.workflowInstances()).allMatch(h -> h.getStatus().isTerminal());
        });

        // Wait 2 seconds before asserting to let async cleanup settle
        try {
            Thread.sleep(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        for (WorkflowContext context : workflowEngine.workflowInstances().stream()
                                                     .map(WorkflowExecution::workflowContext).toList()) {
            assertThat(context.getStatus().isTerminal()).isTrue();
            assertThat(context.getStatus()).isEqualTo(WorkflowStatus.CANCELLED);
            assertThat(context.getStepHistory()).containsExactlyInAnyOrder("stepA", "stepB", "stepC");
        }

        // Verify all expected events were published
        var eventStore = PrettyPrintingRecordingEventStore.lastInstance();
        var eventTypes = eventStore.getPublishedEvents().stream()
                                   .map(e -> e.type().qualifiedName().toString())
                                   .toList();

        eventTypes.forEach(e -> logger.info("  - {}", e));
        assertThat(eventTypes).contains(
                "io.axoniq.dsl.cancel.WorkflowStarted",
                "io.axoniq.workflow.StepAStarted",
                "io.axoniq.workflow.StepBStarted",
                "io.axoniq.workflow.StepCStarted",
                "io.axoniq.workflow.StepACancelled",
                "io.axoniq.workflow.StepBCancelled",
                "io.axoniq.workflow.StepCCancelled",
                "io.axoniq.dsl.cancel.WorkflowCancelled"
        );
    }
}
