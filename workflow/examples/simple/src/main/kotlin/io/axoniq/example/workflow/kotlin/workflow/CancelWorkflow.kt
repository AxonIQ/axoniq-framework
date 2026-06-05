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

import io.axoniq.workflow.dsl.kotlin.Kontext
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlin.time.Duration.Companion.minutes

private val logger = KotlinLogging.logger {}

/**
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class CancelWorkflow {

    fun Kontext.onExecute() {
        logger.info { "Cancel workflow started for $payload" }

        // Launch 3 long-running steps (non-blocking, each simulates 5 min work)
        val fiveMinMs = 5.minutes.inWholeMilliseconds
        val r1 = execute(
            stepName = "stepA",
            timeout = 5.minutes,
            retryPolicy = RetryPolicy.NONE
        ) { _, _ -> Thread.sleep(fiveMinMs); mapOf() }
        val r2 = execute(
            stepName = "stepB",
            timeout = 5.minutes,
            retryPolicy = RetryPolicy.NONE
        ) { _, _ -> Thread.sleep(fiveMinMs); mapOf() }
        val r3 = execute(
            stepName = "stepC",
            timeout = 5.minutes,
            retryPolicy = RetryPolicy.NONE
        ) { _, _ -> Thread.sleep(fiveMinMs); mapOf() }

        // Combine results but don't block on them
        allMatch({ it.isCompleted }, r1, r2, r3)

        // Wait 5 seconds then cancel
        Thread.sleep(5_000)
        logger.info { "Cancelling workflow after 5 seconds" }
        cancel()
    }
}

fun CancelWorkflow.execute(kontext: Kontext) = with(kontext) { onExecute() }
