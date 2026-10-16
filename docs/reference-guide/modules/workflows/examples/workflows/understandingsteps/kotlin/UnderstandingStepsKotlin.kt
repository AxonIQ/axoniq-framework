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

package workflows.understandingsteps.kotlin

import io.axoniq.framework.workflow.dsl.api.retry.RetryPolicy
import io.axoniq.framework.workflow.dsl.kotlin.Kontext
import org.axonframework.messaging.core.unitofwork.ProcessingContext
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

object ShippingService {
    fun shipOrder(pc: ProcessingContext, payload: Map<String, Any?>): Map<String, Any?> = payload
}

object InventoryService {
    fun reserveStock(): Boolean = true
}

fun Kontext.shipAndReserve(address: String) {
    // tag::named-arguments[]
    val result = execute(
        stepName = "shipOrder",
        inputPayload = mapOf("address" to address),
        timeout = 5.minutes,
        retryPolicy = RetryPolicy.NONE
    ) { pc, p ->
        ShippingService.shipOrder(pc, p)
    }

    val reservation = awaitExecute(
        stepName = "reserveStock",
        timeout = 10.seconds
    ) { _, _ ->
        mapOf("reserved" to InventoryService.reserveStock())
    }
    // end::named-arguments[]
}
