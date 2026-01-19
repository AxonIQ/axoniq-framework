package io.axoniq.workflow.runtime;

import io.axoniq.workflow.runtime.context.WorkflowContext;
import io.axoniq.workflow.runtime.context.WorkflowContextImpl;
import io.axoniq.workflow.runtime.definition.WorkflowDefinition;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.engine.WorkflowEngine;
import io.axoniq.workflow.runtime.event.StepCompleted;
import io.axoniq.workflow.runtime.event.StepStarted;
import io.axoniq.workflow.runtime.payload.Payload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

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

  static class UserSignupWorkflow implements WorkflowDefinition {

    @Override
    public void execute(WorkflowContext context) {
      var success = context.execute("createUser", Boolean.class, () -> {
        logger.info("Creating user.");
        return true;
      });
      if (!success) {
        return;
      }
      context.execute("activateUser", () -> {
        logger.info("Activating user.");
      });

      context.execute("sendWelcomeEmail", () -> {
        logger.info("Sending welcome mail to user.");
      });

    }
  }

  @Test
  void shouldExecuteAllStepsOnFirstRun() {
    User user = new User("user-123", "test@example.com");
    String workflowId = "signup002";

    engine.execute(workflowId, new UserSignupWorkflow(), new Payload().withValue("user", user));
    assertEquals(Set.of("createUser", "activateUser", "sendWelcomeEmail"), engine.context.steps.keySet());

    // Verify events published
    var events = stateManager.getEventPayloads(workflowId);
    assertEquals(6, events.size()); // 3 starts + 3 completes
    assertInstanceOf(StepStarted.class, events.get(0));
    assertInstanceOf(StepCompleted.class, events.get(1));
    assertInstanceOf(StepStarted.class, events.get(2));
    assertInstanceOf(StepCompleted.class, events.get(3));
    assertInstanceOf(StepStarted.class, events.get(4));
    assertInstanceOf(StepCompleted.class, events.get(5));
  }


  @Test
  void shouldReturnCachedResultForCompletedSteps() {
    String workflowId = "signup001";

    WorkflowDefinition workflow = (context) -> {
      String value = context.execute("getValue", String.class, () -> "cached-value");
    };

    // First execution
    engine.execute(workflowId, workflow);

    // Verify events after first run
    var eventsAfterFirst = stateManager.getEventPayloads(workflowId);
    assertEquals(2, eventsAfterFirst.size());
    assertInstanceOf(StepStarted.class, eventsAfterFirst.get(0));
    assertInstanceOf(StepCompleted.class, eventsAfterFirst.get(1));
    assertEquals("cached-value", ((Payload) ((StepCompleted) eventsAfterFirst.get(1)).result()).get("__getValue").getAsTyped());

    // Second execution - should return cached value
    engine.execute(workflowId, workflow);

    // Verify no new events on replay
    var eventsAfterSecond = stateManager.getEventPayloads(workflowId);
    assertEquals(2, eventsAfterSecond.size(), "No new events should be published on replay");
  }

}
