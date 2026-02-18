package io.axoniq.example.workflow.autodetected;

import io.axoniq.example.workflow.fixture.MagicHappenedEvent;
import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class UserSignupAutodetectedTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public UserSignupAutodetectedTest() {

        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Consumer<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>> getDeclaredDefinitions() {
        return (d) -> d.autodetected(
                UserSignupWorkflow.class,
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
            assertThat(workflowEngine.workflowInstances().values()).allMatch(h -> h.getStatus().isTerminal());
        });

        // Verify that both workflows executed all steps
        for (WorkflowContext context : workflowEngine.workflowInstances().values().stream()
                                                     .map(WorkflowEngine.ExecutionHandle::workflowContext).toList()) {
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
