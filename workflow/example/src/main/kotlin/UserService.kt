package io.axoniq.example.workflow.kotlin

import io.axoniq.example.workflow.Waiter
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
