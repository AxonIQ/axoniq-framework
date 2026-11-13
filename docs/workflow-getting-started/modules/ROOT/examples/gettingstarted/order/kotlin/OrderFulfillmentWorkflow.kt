/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package gettingstarted.order.kotlin

import gettingstarted.order.InventoryService
import io.axoniq.framework.workflow.annotation.Workflow
import io.axoniq.framework.workflow.dsl.kotlin.Kontext

// tag::workflow[]
class OrderFulfillmentWorkflow {

    @Workflow(idProperty = "orderId", startOnEventName = "OrderPlaced")
    fun Kontext.onExecute() {
        val reservation = awaitExecute("reserveStock") { _, _ ->
            mapOf("reserved" to InventoryService.reserveStock())
        }
        if (reservation["reserved"] != true) {
            fail(RuntimeException("Stock unavailable"))
            return
        }
    }
}
// end::workflow[]
