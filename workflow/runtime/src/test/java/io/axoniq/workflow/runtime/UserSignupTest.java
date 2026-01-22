package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple.SimpleContext;
import io.axoniq.workflow.dsl.simple.SimpleDefinition;
import io.axoniq.workflow.runtime.engine.SimpleStateManager;
import io.axoniq.workflow.runtime.engine.StepStatus;
import io.axoniq.workflow.runtime.engine.WorkflowEngine;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.FilesystemStyleComponentDescriptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

import static io.axoniq.workflow.dsl.simple.Payload.payload;
import static io.axoniq.workflow.runtime.context.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.util.MetadataUtils.create;
import static io.axoniq.workflow.runtime.util.MetadataUtils.getStepStatus;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class UserSignupTest {

  private static final Logger logger = LoggerFactory.getLogger(UserSignupTest.class);

  private SimpleStateManager stateManager;
  private WorkflowEngine engine;

  @BeforeEach
  void setUp() {
    stateManager = new SimpleStateManager();
    engine = new WorkflowEngine(stateManager, stateManager);
  }

  @AfterEach
  void printEvents() {
    var descriptor = new FilesystemStyleComponentDescriptor();
    stateManager.describeTo(descriptor);
    logger.info(descriptor.describe());
  }

  record User(String id, String email) {
  }

  static class UserSignupWorkflow implements SimpleDefinition {

    @Override
    public String association(@Nonnull Map<String, Object> trigger) {
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
    User user = new User("user-123", "test@example.com");

    var workflow = new UserSignupWorkflow();
    var context = engine.restoreAndExecute(workflow, payload("user", user).getValues()).join();
    assertThat(context.getStepHistory()).containsExactlyInAnyOrder("createUser", "activateUser", "sendWelcomeEmail");

    // Verify events published
    var events = stateManager.getHistory(context.getWorkflowId());
    assertThat(events).hasSize(8); // 1 workflow start + 3 starts + 3 completes + 1 workflow completed
    assertThat(getStepStatus(events.get(1).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(2).metadata())).contains(StepStatus.COMPLETED);
    assertThat(getStepStatus(events.get(3).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(4).metadata())).contains(StepStatus.COMPLETED);
    assertThat(getStepStatus(events.get(5).metadata())).contains(StepStatus.STARTED);
    assertThat(getStepStatus(events.get(6).metadata())).contains(StepStatus.COMPLETED);
  }


  @Test
  void shouldReturnCachedResultForCompletedSteps() {
    String workflowId = "signup001";

    class MyWorkflowDefinition implements SimpleDefinition {

      @Override
      public void execute(SimpleContext context) {
        String value = context.execute("getValue", String.class, () -> "cached-value",
          namespace("other.namespace")
            .baseName("getValueStep")
            .stepStarted("Initialized")
        );
      }

      @Override
      public String association(@Nonnull Map<String, Object> trigger) {
        return workflowId;
      }
    }

    var definition = new MyWorkflowDefinition();

    var context = engine.initialize(definition);
    var lifecycle = engine.restore(definition, context);
    engine.execute(definition, context, lifecycle).join();

    var eventsAfterFirst = stateManager.getEventPayloads(workflowId);
    assertThat(eventsAfterFirst).hasSize(4); // 1 workflow start + 1 start + 1 complete + 1 workflow completed
    //noinspection unchecked
    assertEquals("cached-value", ((Map<String, Object>) eventsAfterFirst.get(2)).get("__getValue"));

    engine.execute(definition, context, lifecycle).join();

    var eventsAfterSecond = stateManager.getEventPayloads(workflowId);
    assertThat(eventsAfterSecond).describedAs("No new events should be published on replay").hasSize(4);
  }

}
