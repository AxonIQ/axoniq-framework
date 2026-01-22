package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple.SimpleContext;
import io.axoniq.workflow.dsl.simple.SimpleDefinition;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.Coordinator;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.engine.StepStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static io.axoniq.workflow.runtime.DelayedPublisher.Schedule.ofMillis;
import static io.axoniq.workflow.runtime.util.MetadataUtils.getStepStatus;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class CoordinatorUserSignupTest {

  private static final Logger logger = LoggerFactory.getLogger(CoordinatorUserSignupTest.class);

  private StateManager stateManager;
  private Coordinator coordinator;
  private DelayedPublisher delayedPublisher;

  @BeforeEach
  void setUp() {
    stateManager = new StateManager();
    coordinator = new Coordinator(stateManager);
    delayedPublisher = new DelayedPublisher(stateManager);
  }

  @AfterEach
  void printEvents() {
    stateManager.printPayloads();
  }

  record RegistrationReceivedEvent(String id, String email) {
  }

  public static class UserSignupWorkflow extends SimpleDefinition.Type {

    @Override
    public String workflowId(Map<String, Object> trigger) {
      return "signup-" + trigger.get("id").toString();
    }

    @Override
    public void execute(SimpleContext context) {
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
      logger.info("User signup workflow ended at {} for {}", Instant.now(context.getClock()), context.getPayload());
    }
  }

  @Test
  void shouldExecuteAllStepsOnFirstRun() {

    coordinator.register(UserSignupWorkflow.class, RegistrationReceivedEvent.class);

    delayedPublisher.addSchedules(List.of(
      ofMillis(
        500,
        new RegistrationReceivedEvent("user-456", "kermit@muppets.biz")
      ),
      ofMillis(
        500,
        new RegistrationReceivedEvent("user-123", "kermit@muppets.biz")
      )
    ));

    // Start the coordinator - it should run in the background
    coordinator.start();

    // Arm the publisher to start the delayed execution
    delayedPublisher.start();

    // Wait until both workflows are running concurrently
    await().untilAsserted(() -> {
      assertThat(coordinator.getRunning()).hasSize(2);
    });

    // Wait until both workflows are completed and moved to history
    await().untilAsserted(() -> {
      assertThat(coordinator.getHistory()).hasSize(2);
      assertThat(coordinator.getRunning()).isEmpty();
    });

    // Verify that both workflows executed all steps
    for (WorkflowContext context : coordinator.getHistory()) {
      assertThat(context.getStepHistory()).containsExactlyInAnyOrder("createUser", "activateUser", "sendWelcomeEmail", "waitASecond");

      // Verify events published for this workflow
      var events = stateManager.getHistory(context.getWorkflowId());
      assertThat(events).hasSize(9); // 4 starts + 4 completes/timeouts + 1 workflow completed
      assertThat(getStepStatus(events.get(0).metadata())).contains(StepStatus.STARTED);
      assertThat(getStepStatus(events.get(1).metadata())).contains(StepStatus.COMPLETED);
      assertThat(getStepStatus(events.get(2).metadata())).contains(StepStatus.STARTED);
      assertThat(getStepStatus(events.get(3).metadata())).contains(StepStatus.COMPLETED);
      assertThat(getStepStatus(events.get(4).metadata())).contains(StepStatus.STARTED);
      assertThat(getStepStatus(events.get(5).metadata())).contains(StepStatus.COMPLETED);
      assertThat(getStepStatus(events.get(6).metadata())).contains(StepStatus.STARTED);
      assertThat(getStepStatus(events.get(7).metadata())).contains(StepStatus.TIMED_OUT);
    }

    // Stop the coordinator
    coordinator.stop();
  }

}
