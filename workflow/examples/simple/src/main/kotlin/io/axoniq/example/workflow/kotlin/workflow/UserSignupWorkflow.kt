/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.example.workflow.kotlin.workflow

import io.axoniq.example.workflow.kotlin.fixture.MagicHappenedEvent
import io.axoniq.example.workflow.kotlin.fixture.NotificationService
import io.axoniq.example.workflow.kotlin.fixture.UserService
import io.axoniq.workflow.dsl.kotlin.Kontext
import io.axoniq.workflow.runtime.api.annotation.Workflow
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Instant
import kotlin.time.Duration.Companion.seconds


private val logger = KotlinLogging.logger {}

class UserSignupWorkflow {

    @Workflow(idProperty = "id", startOnEvent = "my.custom.RegistrationReceived")
    fun Kontext.onExecute() {
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

        logger.info { "Magic happened because of the magician ${magic.magician}" }
        logger.info { "User signup workflow ended at ${Instant.now()} for $payload" }
    }
}

fun UserSignupWorkflow.execute(kontext: Kontext) = with(kontext) { onExecute() }
