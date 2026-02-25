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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */

package io.axoniq.dataprotection.sample.giftcard.event

import io.axoniq.framework.dataprotection.api.DataSubjectId
import io.axoniq.framework.dataprotection.api.DeepPersonalData
import io.axoniq.framework.dataprotection.api.PersonalData
import io.axoniq.framework.dataprotection.api.SerializedPersonalData
import org.axonframework.eventsourcing.annotation.EventTag
import java.math.BigDecimal
import java.time.LocalDate

/**
 * Event representing the successful issuance of a new gift card.
 *
 * This event is emitted when a gift card has been successfully created in the system
 * with an initial monetary amount. It represents a fact that has occurred and cannot be
 * changed, following the event sourcing principle of immutable events.
 *
 * Event characteristics:
 * - Immutable - once emitted, the event data cannot be changed
 * - Reproducible - replaying this event will recreate the same state
 * - Self-contained - contains all necessary information about the event
 *
 * @property giftCardId the unique identifier of the newly issued gift card
 * @property amount the initial monetary amount loaded onto the gift card (always positive)
 * @property username encrypted personal data field
 * @property owner deep encrypted person data (name + address)
 * @property dateOfBirth serialized encrypted date of birth
 * @property dateOfBirthEncrypted encrypted representation of date of birth
 * @property randomNumber random number for demonstration
 * @property randomNumberEncrypted encrypted representation of random number
 */
data class GiftCardIssuedEvent(
    @EventTag
    @DataSubjectId(
        group = GiftPersonalDataGroup.GROUP_NAME,
        prefix = GiftPersonalDataGroup.GROUP_PREFIX
    )
    val giftCardId: String,

    val amount: BigDecimal,

    @PersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    val username: String,

    @DeepPersonalData
    val owner: Person,

    @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    val dateOfBirth: LocalDate?,

    val dateOfBirthEncrypted: ByteArray?,

    @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    val randomNumber: Int?,

    val randomNumberEncrypted: String?
) {
    companion object {
        /**
         * Convenience factory method for creating event with all user-provided data.
         * Encrypted field pairs will be automatically populated by the data protection framework.
         */
        fun create(
            giftCardId: String,
            amount: BigDecimal,
            username: String,
            owner: Person,
            dateOfBirth: LocalDate,
            randomNumber: Int
        ): GiftCardIssuedEvent = GiftCardIssuedEvent(
            giftCardId = giftCardId,
            amount = amount,
            username = username,
            owner = owner,
            dateOfBirth = dateOfBirth,
            dateOfBirthEncrypted = null,
            randomNumber = randomNumber,
            randomNumberEncrypted = null
        )
    }
}
