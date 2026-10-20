/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.executesteps.kotlin

import io.axoniq.framework.workflow.dsl.kotlin.Kontext
import kotlin.time.Duration.Companion.seconds

object InventoryService {
    fun reserveStock(): Boolean = true
}

// tag::typed-await-execute[]
fun Kontext.fulfillOrder() {
    val reserved = awaitExecute<Boolean>(
        stepName = "reserveStock",
        timeout = 10.seconds
    ) {
        InventoryService.reserveStock()
    }
}
// end::typed-await-execute[]
