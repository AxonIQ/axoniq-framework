package io.axoniq.workflow.runtime.kotlin

import com.fasterxml.jackson.annotation.JsonProperty
import io.axoniq.workflow.dsl.Kontext
import io.axoniq.workflow.dsl.WorkflowKontextDefinition
import io.axoniq.workflow.runtime.AbstractTestBase
import io.axoniq.workflow.runtime.DelayedPublisher
import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine.ExecutionHandle
import org.assertj.core.api.Assertions
import org.awaitility.Awaitility
import org.awaitility.core.ThrowingRunnable
import org.axonframework.messaging.core.QualifiedName
import org.axonframework.messaging.core.unitofwork.ProcessingContext
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.function.Predicate
import kotlin.time.Duration.Companion.seconds

class UserSignupTest : AbstractTestBase() {

  @JvmRecord
  data class RegistrationReceivedEvent(val id: String, val email: String)

  @JvmRecord
  data class MagicHappenedEvent(
    @field: JsonProperty("magician")
    val magician: String
  )

  object UserService {
    fun createUser(): Boolean {
      logger.info("Creating user.")
      return true
    }

    fun activateUser(pc: ProcessingContext, payload: Map<String, Any?>): Map<String, Any?> {
      val now = Instant.now()
      logger.info("Activating user with id: {}", payload["id"])
      waitWithProgress(1000)
      logger.info("Activation took {}.", Duration.between(Instant.now(), now))
      return mapOf()
    }
  }

  object NotificationService {
    fun sendEmail() {
      logger.info("Sending welcome mail to user.")
    }
  }

  class UserSignupWorkflow : WorkflowKontextDefinition {
    override fun eventNameCustomizer(): EventNameCustomizer {
      return DefaultEventNameCustomizer.Builder.namespace("io.axoniq.dsl.wf")
    }

    override fun workflowId(payload: Map<String, Any?>): String {
      return "signup-" + payload["id"].toString()
    }


    override fun Kontext.onExecute() {
      logger.info("User signup workflow started at {} for {}", Instant.now(), payload)

      val success = awaitExecute("createUser", {
        UserService.createUser()
      })

      if (!success) {
        return
      }

      block {
        execute(
          "activateUser",
          { pc, p -> UserService.activateUser(pc, p) },
          timeout = 10.seconds
        )
      }

      block("waitASecond", 1.seconds)

      block {
        execute("sendWelcomeEmail", { pc, p -> NotificationService.sendEmail(); mapOf() })
      }

      val magic = awaitEvent("waitForMagicToHappen", MagicHappenedEvent::class)

      logger.info("Magic happened because of the magician {}", magic.magician)
      logger.info("User signup workflow ended at {} for {}", Instant.now(), payload)
    }

  }


  @Test
  fun `register user`() {

    workflowRegistry.register(
      QualifiedName(RegistrationReceivedEvent::class.java),
      UserSignupWorkflow()
    )

    delayedPublisher.addSchedules(
      listOf<DelayedPublisher.Schedule>(
        DelayedPublisher.Schedule.ofMillis(
          500,
          RegistrationReceivedEvent("user-456", "kermit@muppets.biz")
        ),
        DelayedPublisher.Schedule.ofMillis(
          6500,
          MagicHappenedEvent("Merlin")
        )
      )
    )


    // Arm the publisher to start the delayed execution
    delayedPublisher.start()


    // all started
    Awaitility.await().untilAsserted(ThrowingRunnable {
      Assertions.assertThat(workflowEngine.workflowInstances()).isNotEmpty()
    })

    // simulate all-replayed and start workflows
    workflowEngine.runWorkflows()


    // run to the end
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(ThrowingRunnable {
      Assertions.assertThat(workflowEngine.workflowInstances().values)
        .allMatch(Predicate { h: ExecutionHandle? -> h!!.status.isTerminal })
    })


    // Verify that both workflows executed all steps
    for (context in workflowEngine.workflowInstances().values.stream()
      .map(ExecutionHandle::workflowContext).toList()) {
      Assertions.assertThat(context.getStatus().isTerminal).isTrue()
      Assertions.assertThat(context.getStatus()).isEqualTo(WorkflowStatus.COMPLETED)
      Assertions.assertThat(context.getStepHistory()).containsExactlyInAnyOrder(
        "createUser", "activateUser",  // "activateUser2",
        "sendWelcomeEmail",
        "waitASecond", "waitForMagicToHappen"
      )
    }


  }
}