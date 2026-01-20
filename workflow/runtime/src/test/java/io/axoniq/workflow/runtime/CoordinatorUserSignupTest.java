package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple.SimpleContext;
import io.axoniq.workflow.dsl.simple.SimpleDefinition;
import io.axoniq.workflow.runtime.engine.Coordinator;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.engine.StepStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      logger.info("User signup workflow started for {}", context.getPayload());
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

    }
  }

  @Test
  void shouldExecuteAllStepsOnFirstRun() {

    coordinator.register(UserSignupWorkflow.class, RegistrationReceivedEvent.class);

    delayedPublisher.addSchedules(List.of(
      ofMillis(
        500,
        new RegistrationReceivedEvent("user-456", "kermit@muppets.biz")
      )
    ));

    // Arm the publisher to start the delayed execution
    delayedPublisher.start();

    // Start the coordinator and wait for all workflows to complete
    coordinator.start().join();

    // No need for await since we've already waited for all workflows to complete
    assertThat(coordinator.getHistory()).isNotEmpty();

    var context = coordinator.getHistory().getFirst();
    assertThat(context.getStepHistory()).containsExactlyInAnyOrder("createUser", "activateUser", "sendWelcomeEmail");

    // Verify events published
    var events = stateManager.getHistory(context.getWorkflowId());
    assertThat(events).hasSize(6); // 3 starts + 3 completes
    assertThat(getStepStatus(events.get(0).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(1).metadata())).contains(StepStatus.COMPLETED);
    assertThat(getStepStatus(events.get(2).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(3).metadata())).contains(StepStatus.COMPLETED);
    assertThat(getStepStatus(events.get(4).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(5).metadata())).contains(StepStatus.COMPLETED);
  }

}
