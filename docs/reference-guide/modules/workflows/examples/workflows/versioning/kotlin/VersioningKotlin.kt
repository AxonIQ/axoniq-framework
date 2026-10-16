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

package workflows.versioning.kotlin

import io.axoniq.framework.workflow.annotation.Workflow
import io.axoniq.framework.workflow.dsl.kotlin.Kontext

data class OrderPlacedEvent(val orderId: String)

object InventoryService {
    fun reserveStock(): Boolean = true
}

object PaymentService {
    fun processV2(): Boolean = true
    fun chargeV1(): Boolean = true
}

class OrderWorkflow {

    // tag::migrate-version[]
    @Workflow(idProperty = "orderId", startOnEventClass = OrderPlacedEvent::class)
    fun Kontext.execute() {
        awaitExecute<Boolean>("reserveStock") { InventoryService.reserveStock() }

        if (migrateVersion("payment-redesign", "0.0.2")) {
            awaitExecute<Boolean>("processPayment") { PaymentService.processV2() }
        } else {
            awaitExecute<Boolean>("chargePayment") { PaymentService.chargeV1() }
        }
    }
    // end::migrate-version[]
}
