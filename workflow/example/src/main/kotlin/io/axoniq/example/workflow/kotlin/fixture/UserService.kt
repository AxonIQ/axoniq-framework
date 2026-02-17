/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.example.workflow.kotlin.fixture

import io.axoniq.example.workflow.fixture.Waiter
import io.github.oshai.kotlinlogging.KotlinLogging
import org.axonframework.messaging.core.unitofwork.ProcessingContext
import java.time.Duration
import java.time.Instant


private val logger = KotlinLogging.logger {}

object UserService {
    fun createUser(): Boolean {
        logger.info { "Creating user." }
        return true
    }

    fun activateUser(pc: ProcessingContext, payload: Map<String, Any?>): Map<String, Any?> {
        val now = Instant.now()
        logger.info { "Activating user with id: ${payload["id"]}" }
        Waiter.waitWithProgress(1000)
        logger.info { "Activation took ${Duration.between(Instant.now(), now)}." }
        return mapOf()
    }
}
