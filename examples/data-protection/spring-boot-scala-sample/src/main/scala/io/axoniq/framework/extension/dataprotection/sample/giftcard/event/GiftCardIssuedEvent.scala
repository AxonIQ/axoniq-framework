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

package io.axoniq.framework.extension.dataprotection.sample.giftcard.event

import io.axoniq.framework.extension.dataprotection.sample.giftcard.GiftPersonalDataGroup
import io.axoniq.framework.extension.dataprotection.api.{DataSubjectId, DeepPersonalData, PersonalData, SerializedPersonalData}
import org.axonframework.eventsourcing.annotation.EventTag

import java.math.BigDecimal
import java.time.LocalDate
import scala.annotation.meta.field

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
 * @param giftCardId the unique identifier of the newly issued gift card
 * @param amount the initial monetary amount loaded onto the gift card (always positive)
 * @param username encrypted personal data field
 * @param owner deep encrypted person data (name + address)
 * @param dateOfBirth serialized encrypted date of birth
 * @param dateOfBirthEncrypted encrypted representation of date of birth
 * @param randomNumber random number for demonstration
 * @param randomNumberEncrypted encrypted representation of random number
 */
case class GiftCardIssuedEvent(
  @EventTag
  @(DataSubjectId @field)(
    group = GiftPersonalDataGroup.GROUP_NAME,
    prefix = GiftPersonalDataGroup.GROUP_PREFIX
  )
  giftCardId: String,

  amount: BigDecimal,

  @(PersonalData @field)(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
  username: String,

  @(DeepPersonalData @field)
  owner: Person,

  @(SerializedPersonalData @field)(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
  dateOfBirth: LocalDate,

  dateOfBirthEncrypted: Array[Byte],

  @(SerializedPersonalData @field)(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
  randomNumber: Int,

  randomNumberEncrypted: String
)

object GiftCardIssuedEvent {
  /**
   * Convenience factory method for creating event with all user-provided data.
   * Encrypted field pairs will be automatically populated by the data protection framework.
   */
  def apply(
    giftCardId: String,
    amount: BigDecimal,
    username: String,
    owner: Person,
    dateOfBirth: LocalDate,
    randomNumber: Int
  ): GiftCardIssuedEvent = {
    GiftCardIssuedEvent(
      giftCardId,
      amount,
      username,
      owner,
      dateOfBirth,
      null,
      randomNumber,
      null
    )
  }
}
