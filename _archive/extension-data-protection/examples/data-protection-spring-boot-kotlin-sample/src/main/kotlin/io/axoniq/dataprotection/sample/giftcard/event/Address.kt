/*
 * Copyright (c) 2010-2025. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.dataprotection.sample.giftcard.event

import io.axoniq.framework.dataprotection.api.PersonalData

/**
 * Address data class containing personal data fields.
 *
 */
data class Address(
    @PersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "removed")
    val line1: String,

    @PersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "removed")
    val line2: String,

    val country: String
)
