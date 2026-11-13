/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.publishingevents

import io.axoniq.framework.workflow.dsl.kotlin.Kontext

fun Kontext.approve(orderId: String, approvedBy: String) {
    // tag::await-publish[]
    awaitPublish("notifyApproved", OrderApproved(orderId, approvedBy))
    // end::await-publish[]
}
