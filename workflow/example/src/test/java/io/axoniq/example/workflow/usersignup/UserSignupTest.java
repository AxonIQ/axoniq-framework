package io.axoniq.example.workflow.usersignup;

import io.axoniq.example.workflow.MagicHappenedEvent;
import io.axoniq.example.workflow.RegistrationReceivedEvent;
import io.axoniq.workflow.dsl.simple2.MyWorkflowContext;
import io.axoniq.workflow.dsl.simple2.MyWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
import io.axoniq.workflow.runtime.test.AbstractTestBase;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.eventName;
import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class UserSignupTest extends AbstractTestBase<MyWorkflowContext> {


    public UserSignupTest() {
        super(MyWorkflowContext.class,
              c -> new MyWorkflowContextFactory(c.getComponent(EventNameCustomizer.class,
                                                               DefaultEventNameCustomizer.Builder::eventName))
        );
    }

    @Override
    protected Consumer<WorkflowModule.WorkflowDefinitionPhase.DefinitionPhase<MyWorkflowContext>> getDefinitions() {
        var workflow = new UserSignupWorkflow();
        return (d) -> d.declarative("User signup workflow")
                       .on(EventCondition.fromType(RegistrationReceivedEvent.class))
                       .workflowDefinition(c -> workflow::execute)
                       .eventNameCustomizer(c -> () -> namespace("io.axoniq.dsl.wf"))
                       .workflowIdProvider(c -> (trigger) -> Optional.of("signup-" + trigger.get("id").toString()))
                       .notCustomized();
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
