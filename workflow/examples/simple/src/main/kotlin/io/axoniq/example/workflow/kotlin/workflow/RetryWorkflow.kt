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
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

class KotlinRetryWorkflow {
    val retryAttempts = AtomicInteger(0)
    val asyncRetryAttempts = AtomicInteger(0)

    fun Kontext.onExecute() {
        // Step 1: awaitExecute with retryPolicy
        awaitExecute(
            stepName = "retryStep",
            timeout = 5.seconds,
            retryPolicy = RetryPolicy.maxRetries(3)
        ) { _, _ ->
            val attempt = retryAttempts.incrementAndGet()
            if (attempt < 3) {
                throw RuntimeException("Retry attempt $attempt")
            }
            mapOf("status" to "success")
        }

        // Step 2: execute with retryPolicy
        val result = execute(
            stepName = "asyncRetryStep",
            timeout = 5.seconds,
            retryPolicy = RetryPolicy.maxRetries(2)
        ) { _, _ ->
            val attempt = asyncRetryAttempts.incrementAndGet()
            if (attempt < 2) {
                throw RuntimeException("Async retry attempt $attempt")
            }
            mapOf("status" to "success")
        }

        result.await()
    }
}

fun KotlinRetryWorkflow.execute(kontext: Kontext) = with(kontext) { onExecute() }
