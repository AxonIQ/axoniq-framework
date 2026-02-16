package io.axoniq.example.workflow.kotlin.usersignup;

import io.axoniq.example.workflow.kotlin.MagicHappenedEvent
import io.axoniq.example.workflow.kotlin.NotificationService
import io.axoniq.example.workflow.kotlin.UserService
import io.axoniq.workflow.dsl.kotlin.Kontext
import io.axoniq.workflow.dsl.kotlin.WorkflowKontextDefinition
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Instant
import kotlin.time.Duration.Companion.seconds


private val logger = KotlinLogging.logger {}

class UserSignupWorkflow : WorkflowKontextDefinition {

    override fun Kontext.onExecute() {
        logger.info { "User signup workflow started at ${Instant.now()} for $payload" }

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

        logger.info { "Magic happened because of the magician $magic.magician" }
        logger.info { "User signup workflow ended at ${Instant.now()} for $payload" }
    }

}
