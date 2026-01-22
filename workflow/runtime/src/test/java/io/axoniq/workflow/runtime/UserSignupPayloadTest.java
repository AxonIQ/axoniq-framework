package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple.SimpleContext;
import io.axoniq.workflow.dsl.simple.SimpleDefinition;
import io.axoniq.workflow.runtime.engine.Coordinator;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.engine.StepStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
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

import static io.axoniq.workflow.dsl.simple.Payload.payload;
import static io.axoniq.workflow.runtime.DelayedPublisher.Schedule.ofMillis;
import static io.axoniq.workflow.runtime.util.MetadataUtils.getStepStatus;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class UserSignupPayloadTest {

  private static final Logger logger = LoggerFactory.getLogger(UserSignupPayloadTest.class);

  private StateManager stateManager;
  private DelayedPublisher delayedPublisher;
  private Coordinator coordinator;

  record EmailConfirmed(String userId, String email) {
  }

  @BeforeEach
  void setUp() {
    stateManager = new StateManager();
    coordinator = new Coordinator(stateManager);
    delayedPublisher = new DelayedPublisher(stateManager);
  }

  @AfterEach
  void printEvents() {
    stateManager.printPayloads();
    coordinator.stop();
  }

  record User(String id, String email) {
  }

  public static class UserSignupWorkflow implements SimpleDefinition {

    @Override
    public String association(@Nonnull Map<String, Object> trigger) {
      return "signup-" + trigger.getOrDefault("id", UUID.randomUUID()).toString();
    }

    @Override
    public void execute(@Nonnull SimpleContext context) {

      var startParams = payload(context);
      logger.info("Starting user signup workflow with payload {}", startParams);

      var createdUser = context.execute("createUser",
        startParams,
        payload -> {
          logger.info("Crating user.");
          return payload()
            .with("created", Instant.now(context.getClock()))
            .with("success", true);
        });

      Boolean success = createdUser.get("success");

      if (!success) {
        return;
      }

      var activated = context.execute("activateUser",
        startParams,
        payload -> {
          User user = payload.getPayloadAs("user", context.payloadToTypeConverter(User.class));
          logger.info("Activating user {}.", user.id);
          return payload()
            .with("email", user.email)
            .with("userid", user.id)
            ;
        });

      String activatedEmail = activated.get("email");
      String correlationUserId = activated.get("userid");

      try {

        var confirmed = context.waitForEvent("emailConfirmed",
          EmailConfirmed.class,
          e -> e.userId.equals(correlationUserId),
          Duration.ofSeconds(2)
        );
        if (confirmed.email.equals(activatedEmail)) {
          context.wait("blocked500ms", Duration.ofMillis(500));
          context.execute("sendWelcomeEmail",
            payload("email", activatedEmail),
            (p) -> {
              logger.info("Sending welcome mail to user.");
              return payload("sent", true);
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

  record UserSignedUp(
    User user
  ) {

  }

  @Test
  void shouldExecuteAllStepsOnManualRun() {
    User user = new User("user-123", "test@example.com");

    coordinator.declarative()
      .register(new QualifiedName(UserSignedUp.class), new UserSignupWorkflow());

    delayedPublisher.addSchedules(List.of(
      ofMillis(500,
        new UserSignedUp(user)
      ),
      ofMillis(
        500,
        new EmailConfirmed("user-456", "kermit@muppets.biz") // wrong event, filtered by the predicate
      ),
      ofMillis(
        500,
        new EmailConfirmed(user.id(), user.email())
      )
    ));

    coordinator.start();
    delayedPublisher.start();

    await().untilAsserted(() -> {
      assertThat(coordinator.getHistory()).isNotEmpty();
    });

    var context = coordinator.getHistory().getFirst();

    assertThat(context.getStepHistory()).containsExactlyInAnyOrderElementsOf(
      Set.of("createUser", "activateUser", "emailConfirmed", "blocked500ms", "sendWelcomeEmail")
    );

    // Verify events published
    var events = stateManager.getHistory(context.getWorkflowId());
    assertThat(events).hasSize(12); // 1 workflow start + 5 starts + 5 completes + 1 workflow completed
    assertThat(getStepStatus(events.get(1).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(2).metadata())).contains(StepStatus.COMPLETED);
    assertThat(getStepStatus(events.get(3).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(4).metadata())).contains(StepStatus.COMPLETED);
    assertThat(getStepStatus(events.get(5).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(6).metadata())).contains(StepStatus.COMPLETED);
    assertThat(getStepStatus(events.get(7).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(8).metadata())).contains(StepStatus.TIMED_OUT);
    assertThat(getStepStatus(events.get(9).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(10).metadata())).contains(StepStatus.COMPLETED);
  }
}
