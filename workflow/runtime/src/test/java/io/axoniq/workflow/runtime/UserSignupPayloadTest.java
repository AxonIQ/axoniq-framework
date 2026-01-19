package io.axoniq.workflow.runtime;

import io.axoniq.workflow.runtime.DelayedPublisher.Schedule;
import io.axoniq.workflow.runtime.context.WorkflowContextImpl;
import io.axoniq.workflow.runtime.definition.WorkflowDefinition;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.engine.WorkflowEngine;
import io.axoniq.workflow.runtime.event.StepCompleted;
import io.axoniq.workflow.runtime.event.StepStarted;
import io.axoniq.workflow.runtime.payload.Payload;
import io.axoniq.workflow.runtime.payload.TypedValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static io.axoniq.workflow.runtime.payload.Payload.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class UserSignupPayloadTest {

  private static final Logger logger = LoggerFactory.getLogger(UserSignupPayloadTest.class);

  private StateManager stateManager;
  private WorkflowEngine engine;
  private DelayedPublisher delayedPublisher;

  record EmailConfirmed(String userId, String email) {

  }


  @BeforeEach
  void setUp() {
    stateManager = new StateManager();
    engine = new WorkflowEngine(stateManager);
    delayedPublisher = new DelayedPublisher(stateManager);
  }

  @AfterEach
  void printEvents() {
    stateManager.printPayloads();
  }

  record User(String id, String email) {
  }

  static class UserSignupWorkflow implements WorkflowDefinition {

    @Override
    public void execute(WorkflowContextImpl context) {

      var startParams = context.getPayload();
      logger.info("Starting user signup workflow with payload {}", startParams);

      var createdUser = context.execute("createUser",
        empty(),
        payload -> {
          logger.info("Crating user.");
          return new Payload()
            .withValue("created", Instant.class, Instant.now())
            .withValue("success", Boolean.class, true)
            ;
        });

      Boolean success = createdUser.get("success").getAsTyped();

      if (!success) {
        return;
      }

      var activated = context.execute("activateUser",
        createdUser,
        payload -> {
          User user = payload.get("user").getAsTyped();
          logger.info("Activating user {}.", user.id);
          return new Payload()
            .withValue("email", user.email)
            .withValue("userid", user.id)
            ;
        });

      String activatedEmail = activated.get("email").getAsTyped();
      String correlationUserId = activated.get("userid").getAsTyped();

      try {

        var confirmed = context.waitFor("confirmedEmail",
          EmailConfirmed.class,
          Duration.ofSeconds(2),
          e -> e.userId.equals(correlationUserId)
        );
        if (confirmed.email.equals(activatedEmail)) {
          context.execute("sendWelcomeEmail", () -> {
            logger.info("Sending welcome mail to user.");
          });
        } else {
          logger.info("Welcome mail not sent. {} != {}", confirmed.email, activatedEmail);
        }
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }

      var payload = context.getPayload();
      logger.info("Finished workflow: {}", payload);
    }
  }

  @Test
  void shouldExecuteAllStepsOnFirstRun() {
    User user = new User("user-123", "test@example.com");
    String workflowId = "signup-" + user.id();
    Payload payload = new Payload().withValue("user", user);

    // Schedule the EmailConfirmed event to be published after a short delay
    delayedPublisher.addSchedules(List.of(
      new Schedule(
        Duration.ofMillis(500),
        new EmailConfirmed("user-456", "kermit@muppets.biz")
      ),
      new Schedule(
        Duration.ofMillis(500),
        new EmailConfirmed(user.id(), user.email())
      )
    ));

    // Arm the publisher to start the delayed execution
    delayedPublisher.arm();

    // Execute the workflow
    engine.execute(workflowId, new UserSignupWorkflow(), payload);

    assertEquals(Set.of("createUser", "activateUser", "confirmedEmail", "sendWelcomeEmail"), engine.context.steps.keySet());

    // Verify events published
    var events = stateManager.getEventPayloads(workflowId);
    assertEquals(8, events.size()); // 4 starts + 4 completes
    assertInstanceOf(StepStarted.class, events.get(0));
    assertInstanceOf(StepCompleted.class, events.get(1));
    assertInstanceOf(StepStarted.class, events.get(2));
    assertInstanceOf(StepCompleted.class, events.get(3));
    assertInstanceOf(StepStarted.class, events.get(4));
    assertInstanceOf(StepCompleted.class, events.get(5));
    assertInstanceOf(StepStarted.class, events.get(6));
    assertInstanceOf(StepCompleted.class, events.get(7));
  }


}
