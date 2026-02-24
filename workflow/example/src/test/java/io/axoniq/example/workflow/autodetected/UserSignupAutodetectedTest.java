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
package io.axoniq.example.workflow.autodetected;

import io.axoniq.example.workflow.fixture.MagicHappenedEvent;
import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class UserSignupAutodetectedTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public UserSignupAutodetectedTest() {

        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>> getDeclaredDefinitions() {
        return (d) -> d.autodetected(
                c -> new UserSignupWorkflow(),
                SimpleWorkflowContext.class
        );
    }

    @Test
    void shouldExecuteAllStepsOnFirstRun() {

        delayedPublisher.addSchedules(List.of(
                ofMillis(
                        500,
                        new RegistrationReceivedEvent("user-456", "kermit@muppets.biz")
                ),
                ofMillis(
                        6500,
                        new MagicHappenedEvent("Merlin")
                )
        ));

        // Arm the publisher to start the delayed execution
        delayedPublisher.start();

        // all started
        await().untilAsserted(() -> {
            assertThat(workflowEngine.workflowInstances()).isNotEmpty();
        });

        // simulate all-replayed and start workflows
        workflowEngine.runWorkflows();

        // run to the end
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowEngine.workflowInstances()).allMatch(h -> h.getStatus().isTerminal());
        });

        // Verify that both workflows executed all steps
        for (WorkflowContext context : workflowEngine.workflowInstances().stream()
                                                     .map(WorkflowExecution::workflowContext).toList()) {
            assertThat(context.getStatus().isTerminal()).isTrue();
            assertThat(context.getStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(context.getStepHistory()).containsExactlyInAnyOrder("createUser", "activateUser",
                    // "activateUser2",
                                                                           "sendWelcomeEmail",
                                                                           "waitASecond", "waitForMagicToHappen"
            );
            assertThat(context.getPayload().containsKey("magic"));
            assertThat(context.getPayload().containsKey("__createUser"));
        }
    }
}
