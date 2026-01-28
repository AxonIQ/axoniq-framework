package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple.TestDefinition;
import io.axoniq.workflow.dsl.simple.TestWorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.EventBasedWorkflowEngine;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.axoniq.workflow.runtime.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class WorkflowCoordinatorUserSignupTest extends AbstractTestBase {

  record RegistrationReceivedEvent(String id, String email) {
  }

  record MagicHappenedEvent(String magician) {
  }

  public static class UserSignupWorkflow implements TestDefinition {

    @Override
    public String association(@Nonnull Map<String, Object> trigger) {
      return "signup-" + trigger.get("id").toString();
    }

    @Override
    public void execute(TestWorkflowContext context) {
      logger.info("User signup workflow started at {} for {}", Instant.now(context.getClock()), context.getPayload());
      var success = context.execute("createUser", Boolean.class, () -> {
        logger.info("Creating user.");
        return true;
      });
      if (!success) {
        return;
      }
      context.execute("activateUser",
        () -> {
          logger.info("Activating user.");
        });

      context.execute("sendWelcomeEmail", () -> {
        logger.info("Sending welcome mail to user.");
      });

      context.wait("waitASecond", Duration.ofSeconds(1));

      var magic = context.waitForEvent("waitForMagicToHappen", MagicHappenedEvent.class);
      logger.info("Magic happened because of the magician {}", magic.magician);

      logger.info("User signup workflow ended at {} for {}", Instant.now(context.getClock()), context.getPayload());
    }
  }

  @Test
  void shouldExecuteAllStepsOnFirstRun() {

    workflowRegistry.register(
      new QualifiedName(RegistrationReceivedEvent.class),
      new UserSignupWorkflow()
    );

    delayedPublisher.addSchedules(List.of(
      ofMillis(
        500,
        new RegistrationReceivedEvent("user-456", "kermit@muppets.biz")
      ),
      ofMillis(
        2500,
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
    await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(workflowEngine.workflowInstances().values()).allMatch(h -> h.getStatus().isTerminal());
    });

    // Verify that both workflows executed all steps
    for (WorkflowContext context : workflowEngine.workflowInstances().values().stream().map(EventBasedWorkflowEngine.ExecutionHandle::getContext).toList()) {
      assertThat(context.getStepHistory()).containsExactlyInAnyOrder("createUser", "activateUser", "sendWelcomeEmail", "waitASecond", "waitForMagicToHappen");
    }
  }

}
