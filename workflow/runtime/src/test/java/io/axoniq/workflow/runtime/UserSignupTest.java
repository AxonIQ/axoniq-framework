package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple.SimpleContext;
import io.axoniq.workflow.dsl.simple.SimpleDefinition;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.engine.StepStatus;
import io.axoniq.workflow.runtime.engine.WorkflowEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

import static io.axoniq.workflow.runtime.util.MetadataUtils.getStepStatus;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class UserSignupTest {

  private static final Logger logger = LoggerFactory.getLogger(UserSignupTest.class);

  private StateManager stateManager;
  private WorkflowEngine engine;

  @BeforeEach
  void setUp() {
    stateManager = new StateManager();
    engine = new WorkflowEngine(stateManager);
  }

  @AfterEach
  void printEvents() {
    stateManager.printPayloads();
  }

  record User(String id, String email) {
  }

  static class UserSignupWorkflow extends SimpleDefinition.Type {

    @Override
    public String workflowId(Map<String, Object> trigger) {
      if (trigger.containsKey("id")) {
        return "signing-" + trigger.get("id");
      } else {
        return "signing-" + UUID.randomUUID();
      }
    }

    @Override
    public void execute(SimpleContext context) {
      var success = context.execute("createUser", Boolean.class, () -> {
        logger.info("Creating user.");
        return true;
      });
      if (!success) {
        return;
      }
      context.execute("activateUser",
        () -> { //ActivationOfUserStarted -> //ActivateUserStarted -> //StepStartedEvent
          logger.info("Activating user.");
        });//ActivateUserCompleted  -> metadata stepType//StepCompletedEvent

      context.execute("sendWelcomeEmail", () -> {
        logger.info("Sending welcome mail to user.");
      });

    }
  }

  @Test
  void shouldExecuteAllStepsOnFirstRun() {
    User user = new User("user-123", "test@example.com");

    var context = engine.execute(new UserSignupWorkflow(), Map.of("user", user)).join();
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


  @Test
  void shouldReturnCachedResultForCompletedSteps() {
    String workflowId = "signup001";

    class MyWorkflowDefinition extends SimpleDefinition.Type {

      @Override
      public void execute(SimpleContext context) {
        String value = context.execute("getValue", String.class, () -> "cached-value");
      }

      @Override
      public String workflowId(Map<String, Object> trigger) {
        return workflowId;
      }
    }

    var definition = new MyWorkflowDefinition();

    engine.execute(definition).join();

    var eventsAfterFirst = stateManager.getEventPayloads(workflowId);
    assertThat(eventsAfterFirst).hasSize(2);
    //noinspection unchecked
    assertEquals("cached-value", ((Map<String, Object>) eventsAfterFirst.get(1)).get("__getValue"));

    engine.execute(definition).join();

    var eventsAfterSecond = stateManager.getEventPayloads(workflowId);
    assertThat(eventsAfterSecond).describedAs("No new events should be published on replay").hasSize(2);
  }

}
