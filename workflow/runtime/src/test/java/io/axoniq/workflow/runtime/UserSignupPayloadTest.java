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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static io.axoniq.workflow.runtime.DelayedPublisher.Schedule.ofMillis;
import static io.axoniq.workflow.runtime.util.MetadataUtils.getStepStatus;
import static org.assertj.core.api.Assertions.assertThat;

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

  public static class UserSignupWorkflow extends SimpleDefinition.Type {

    @Override
    public String workflowId(Map<String, Object> trigger) {
      if (trigger != null && trigger.containsKey("id")) {
        return "signup-" + trigger.get("id").toString();
      }
      return "signup-" + UUID.randomUUID();
    }

    @Override
    public void execute(SimpleContext context) {

      var startParams = context.getPayload();
      logger.info("Starting user signup workflow with payload {}", startParams);

      var createdUser = context.execute("createUser",
        startParams,
        payload -> {
          logger.info("Crating user.");
          return Map.of(
            "created", Instant.now(context.getClock()),
            "success", true
          );
        });

      Boolean success = (Boolean) createdUser.get("success");

      if (!success) {
        return;
      }

      var activated = context.execute("activateUser",
        startParams,
        payload -> {
          User user = (User) payload.get("user");
          logger.info("Activating user {}.", user.id);
          return Map.of(
            "email", user.email,
            "userid", user.id
          );
        });

      String activatedEmail = (String) activated.get("email");
      String correlationUserId = (String) activated.get("userid");

      try {

        var confirmed = context.waitForEvent("emailConfirmed",
          EmailConfirmed.class,
          e -> e.userId.equals(correlationUserId),
          Duration.ofSeconds(2)
        );
        if (confirmed.email.equals(activatedEmail)) {
          context.wait("blocked500ms", Duration.ofMillis(500));
          context.execute("sendWelcomeEmail",                                 // sendWelcomeEmailStarted(email=asasa@dfdfd.de), , metadata{type=StepStarted, workflowId=4711}
            Map.of("email", activatedEmail),
            (p) -> {
              logger.info("Sending welcome mail to user.");
              return Map.of("sent", true);                // sendWelcomeEmailCompleted(sent=true), metadata{type=StepCompleted, workflowId=4711}
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
  void shouldExecuteAllStepsOnManualRun() {
    User user = new User("user-123", "test@example.com");
    var payload = Map.<String, Object>of("user", user);

    delayedPublisher.addSchedules(List.of(
      ofMillis(
        500,
        new EmailConfirmed("user-456", "kermit@muppets.biz") // wrong util, filtered by the predicate
      ),
      ofMillis(
        500,
        new EmailConfirmed(user.id(), user.email())
      )
    ));

    // Arm the publisher to start the delayed execution
    delayedPublisher.start();

    var context = engine.execute(new UserSignupWorkflow(), payload).join();

    assertThat(context.getStepHistory()).containsExactlyInAnyOrderElementsOf(
      Set.of("createUser", "activateUser", "emailConfirmed", "blocked500ms", "sendWelcomeEmail")
    );

    // Verify events published
    var events = stateManager.getHistory(context.getWorkflowId());
    assertThat(events).hasSize(10); // 5 starts + 5 completes
    assertThat(getStepStatus(events.get(0).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(1).metadata())).contains(StepStatus.COMPLETED);
    assertThat(getStepStatus(events.get(2).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(3).metadata())).contains(StepStatus.COMPLETED);
    assertThat(getStepStatus(events.get(4).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(5).metadata())).contains(StepStatus.COMPLETED);
    assertThat(getStepStatus(events.get(6).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(7).metadata())).contains(StepStatus.TIMED_OUT);
    assertThat(getStepStatus(events.get(8).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(9).metadata())).contains(StepStatus.COMPLETED);
  }
}
