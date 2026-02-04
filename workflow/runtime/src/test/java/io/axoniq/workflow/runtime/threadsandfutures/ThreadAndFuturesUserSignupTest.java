package io.axoniq.workflow.runtime.threadsandfutures;

import io.axoniq.workflow.dsl.simple.TestDefinition;
import io.axoniq.workflow.dsl.simple.TestWorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.impl.threadsandfutures.MultiThreadedWorkflowEngine;
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

class ThreadAndFuturesUserSignupTest extends AbstractTestBase {

  record RegistrationReceivedEvent(String id, String email) {
  }

  record MagicHappenedEvent(String magician) {
  }

  static class UserService {
    static boolean createUser() {
      logger.info("Creating user.");
      return true;
    }

    static Map<String, Object> activateUser(Map<String, Object> payload) {
      Instant now = Instant.now();
      logger.info("Activating user with id: {}", payload.get("id"));
      waitWithProgress(1_000);
      logger.info("Activation took {}.", Duration.between(Instant.now(), now));
      return Map.of();
    }
  }

  static class NotificationService {
    static void sendEmail() {
      logger.info("Sending welcome mail to user.");
    }
  }


  public static class UserSignupWorkflow implements TestDefinition {

    @Override
    public String association(@Nonnull Map<String, Object> trigger) {
      return "signup-" + trigger.get("id").toString();
    }

    @Override
    public void execute(@Nonnull TestWorkflowContext ctx) {

      logger.info("User signup workflow started at {} for {}", Instant.now(), ctx.getPayload());

      // -> start
      var success = ctx.execute("createUser", Boolean.class, UserService::createUser);
      if (!success) {
        return;
      }
      ctx.execute("activateUser", ctx.getPayload(), UserService::activateUser, Duration.ofSeconds(10));
      ctx.execute("sendWelcomeEmail", NotificationService::sendEmail);
      ctx.wait("waitASecond", Duration.ofSeconds(1));
      var magic = ctx.waitForEvent("waitForMagicToHappen", MagicHappenedEvent.class, Duration.ofSeconds(5));
      logger.info("Magic happened because of the magician {}", magic.magician);
      // -> end

      logger.info("User signup workflow ended at {} for {}", Instant.now(), ctx.getPayload());
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
    for (WorkflowContext context : workflowEngine.workflowInstances().values().stream().map(MultiThreadedWorkflowEngine.ExecutionHandle::getContext).toList()) {
      assertThat(context.getStepHistory()).containsExactlyInAnyOrder("createUser", "activateUser", "sendWelcomeEmail", "waitASecond", "waitForMagicToHappen");
    }
  }

}
