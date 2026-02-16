package io.axoniq.example.workflow.kotlin

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

object NotificationService {
    fun sendEmail() {
        logger.info { "Sending welcome mail to user." }
    }
}
