package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple2.MyWorkflowContext;
import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
import io.axoniq.workflow.runtime.test.AbstractTestBase;
import io.axoniq.workflow.runtime.test.fixture.MagicHappenedEvent;
import io.axoniq.workflow.runtime.test.fixture.NotificationService;
import io.axoniq.workflow.runtime.test.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.runtime.test.fixture.UserService;
import jakarta.annotation.Nonnull;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class TaskQueueUserSignupTest extends AbstractTestBase {

  public static class UserSignupWorkflow {

    public void execute(@Nonnull MyWorkflowContext ctx) {

      Logger logger = LoggerFactory.getLogger(UserSignupWorkflow.class);

      logger.info("User signup workflow started at {} for {}", Instant.now(), ctx.getPayload());

      // -> start
      var success = ctx.execute("createUser", Boolean.class, UserService::createUser);
      if (!success) {
        return;
      }

      ctx.execute("activateUser", ctx.getPayload(), UserService::activateUser, Duration.ofSeconds(10));

      /**
       var a1 = ctx.executeWithResult("activateUser", payload().set("id", "id1").getValues(), UserService::activateUser, Duration.ofSeconds(10));
       var a2 = ctx.executeWithResult("activateUser2", payload().set("id", "id2").getValues(), UserService::activateUser, Duration.ofSeconds(10));
       all(a1, a2).isSuccess();
       */


      ctx.execute("sendWelcomeEmail", NotificationService::sendEmail);
      ctx.wait("waitASecond", Duration.ofSeconds(1L));

      var magic = ctx.waitForEvent("waitForMagicToHappen", MagicHappenedEvent.class, Duration.ofSeconds(5));
      ctx.addPayload(magic);

      logger.info("Magic happened because of the magician {}", magic.magician());
      // -> end

      logger.info("User signup workflow ended at {} for {}", Instant.now(), ctx.getPayload());
    }
  }

  @Override
  protected Consumer<WorkflowModule.WorkflowDefinitionPhase.DefinitionPhase<MyWorkflowContext>> getDefinitions() {
    var workflow = new UserSignupWorkflow();
    return (d) -> d.declarative("User signup workflow")
      .on(EventCondition.fromType(RegistrationReceivedEvent.class))
      .workflowDefinition(c -> workflow::execute)
      .eventNameCustomizer(c -> () -> namespace("io.axoniq.dsl.wf"))
      .workflowIdProvider(c -> (trigger) -> Optional.of("signup-" + trigger.get("id").toString()))
      .notCustomized();
  }


  @Test
  void shouldExecuteAllStepsOnFirstRun() {

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
    for (WorkflowContext context : workflowEngine.workflowInstances().values().stream().map(WorkflowEngine.ExecutionHandle::workflowContext).toList()) {
      assertThat(context.getStatus().isTerminal()).isTrue();
      assertThat(context.getStatus()).isEqualTo(WorkflowStatus.COMPLETED);
      assertThat(context.getStepHistory()).containsExactlyInAnyOrder("createUser", "activateUser",
        // "activateUser2",
        "sendWelcomeEmail",
        "waitASecond", "waitForMagicToHappen"
      );
      assertThat(context.getPayload().containsKey("magic"));
      assertThat(context.getPayload().containsKey("__createUser"));
    }
  }

}
