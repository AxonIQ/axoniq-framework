package io.axoniq.workflow.runtime.kotlin

import io.axoniq.workflow.dsl.kotlin.*
import io.axoniq.workflow.dsl.simple.Payload.payload
import io.axoniq.workflow.runtime.DelayedPublisher
import io.axoniq.workflow.runtime.DelayedPublisher.Schedule
import io.axoniq.workflow.runtime.engine.StateManager
import io.axoniq.workflow.runtime.engine.StepStatus
import io.axoniq.workflow.runtime.engine.WorkflowEngine
import io.axoniq.workflow.runtime.util.MetadataUtils.getStepStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.*

private val logger = KotlinLogging.logger {}

internal class UserSignupPayloadTest {
  private lateinit var stateManager: StateManager
  private lateinit var engine: WorkflowEngine
  private lateinit var delayedPublisher: DelayedPublisher

  internal data class EmailConfirmed(val userId: String, val email: String)

  @BeforeEach
  fun setUp() {
    stateManager = StateManager()
    engine = WorkflowEngine(stateManager)
    delayedPublisher = DelayedPublisher(stateManager)
  }

  @AfterEach
  fun printEvents() {
    stateManager.printPayloads()
  }

  internal data class User(val id: String, val email: String)

  class UserSignupWorkflow : KDefinition(
    associate = { trigger -> "signup-${trigger.getOrDefault("id", UUID.randomUUID())}" },
    execute = { context: Kontext ->
      with(context) {

        val startParams = payload(context)
        logger.info { "Starting user signup workflow with payload $startParams" }

        val createdUser = execute(
          "createUser",
          startParams
        ) {
          logger.info { "Crating user." }
          payload()
            .with("created", Instant.now(context.clock))
            .with("success", true)
        }

        val success: Boolean = createdUser.get("success")

        if (!success) {
          return@with
        }

        val activated = execute(
          "activateUser",
          startParams
        ) { payload ->
          val user = payload.get<User>("user")
          logger.info { "Activating user ${user.id}." }
          payload()
            .with("email", user.email)
            .with("userid", user.id)
        }

        val activatedEmail: String = activated.get("email")
        val correlationUserId: String = activated.get("userid")

        try {
          val confirmed = waitForEvent<EmailConfirmed>(
            "emailConfirmed",
            { e -> e.userId == correlationUserId },
            Duration.ofSeconds(2)
          )
          if (confirmed.email == activatedEmail) {
            wait("blocked500ms", Duration.ofMillis(500))
            execute("sendWelcomeEmail", payload("email", activatedEmail)) { _ ->
              logger.info { "Sending welcome mail to user." }
              payload("sent", true)
            }
          } else {
            logger.info { "Welcome mail not sent. ${confirmed.email} != $activatedEmail" }
          }
        } catch (e: Exception) {
          logger.error(e) { e.message }
        }

        val payload = context.payload
        logger.info { "Finished workflow: $payload" }
      }
    }
  )

  @Test
  fun shouldExecuteAllStepsOnManualRun() {
    val user = User("user-123", "test@example.com")
    val payload = payload().with("user", user)

    delayedPublisher.addSchedules(
      listOf(
        Schedule.ofMillis(
          500,
          EmailConfirmed(
            "user-456",
            "kermit@muppets.biz"
          ) // wrong event, filtered by the predicate
        ),
        Schedule.ofMillis(
          500,
          EmailConfirmed(user.id, user.email)
        )
      )
    )

    delayedPublisher.start()

    val workflow = UserSignupWorkflow()
    var context = engine.restoreAndExecute<Kontext>(workflow, payload.values).join()

    assertThat(context.stepHistory).containsExactlyInAnyOrder(
      "createUser", "activateUser", "emailConfirmed", "blocked500ms", "sendWelcomeEmail"
    )

    // Verify events published
    val events = stateManager.getHistory(context.workflowId)
    assertThat(events).hasSize(11) // 5 starts + 5 completes + 1 workflow completed
    assertThat(getStepStatus(events[0]!!.metadata())).contains(StepStatus.STARTED)
    assertThat(getStepStatus(events[1]!!.metadata())).contains(StepStatus.COMPLETED)
    assertThat(getStepStatus(events[2]!!.metadata())).contains(StepStatus.STARTED)
    assertThat(getStepStatus(events[3]!!.metadata())).contains(StepStatus.COMPLETED)
    assertThat(getStepStatus(events[4]!!.metadata())).contains(StepStatus.STARTED)
    assertThat(getStepStatus(events[5]!!.metadata())).contains(StepStatus.COMPLETED)
    assertThat(getStepStatus(events[6]!!.metadata())).contains(StepStatus.STARTED)
    assertThat(getStepStatus(events[7]!!.metadata())).contains(StepStatus.TIMED_OUT)
    assertThat(getStepStatus(events[8]!!.metadata())).contains(StepStatus.STARTED)
    assertThat(getStepStatus(events[9]!!.metadata())).contains(StepStatus.COMPLETED)
  }
}
