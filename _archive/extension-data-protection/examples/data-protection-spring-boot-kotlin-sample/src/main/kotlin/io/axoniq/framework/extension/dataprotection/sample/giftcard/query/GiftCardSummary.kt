/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.extension.dataprotection.sample.giftcard.query

import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.GiftPersonalDataGroup
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.Person
import io.axoniq.framework.extension.dataprotection.api.DeepPersonalData
import io.axoniq.framework.extension.dataprotection.api.PersonalData
import io.axoniq.framework.extension.dataprotection.api.SerializedPersonalData
import java.math.BigDecimal
import java.time.LocalDate

/**
 * Read model representation of a gift card's current state.
 *
 */
class GiftCardSummary() {

    var giftCardId: String? = null

    var remainingValue: BigDecimal? = null

    var initialValue: BigDecimal? = null

    @PersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    var username: String? = null

    @DeepPersonalData
    var owner: Person? = null

    @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    var dateOfBirth: LocalDate? = null

    @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    var randomNumber: Int? = null

    constructor(
        giftCardId: String,
        remainingValue: BigDecimal,
        initialValue: BigDecimal,
        username: String?,
        owner: Person?,
        dateOfBirth: LocalDate?,
        randomNumber: Int?
    ) : this() {
        this.giftCardId = giftCardId
        this.remainingValue = remainingValue
        this.initialValue = initialValue
        this.username = username
        this.owner = owner
        this.dateOfBirth = dateOfBirth
        this.randomNumber = randomNumber
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is GiftCardSummary) return false
        return giftCardId == other.giftCardId
    }

    override fun hashCode(): Int {
        return giftCardId?.hashCode() ?: 0
    }

    override fun toString(): String {
        return "GiftCardSummary(giftCardId='$giftCardId', remainingValue=$remainingValue, " +
                "initialValue=$initialValue, username='$username', owner=$owner, " +
                "dateOfBirth=$dateOfBirth, randomNumber=$randomNumber)"
    }
}
