/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
