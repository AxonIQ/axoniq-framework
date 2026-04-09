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

import io.axoniq.framework.extension.dataprotection.sample.giftcard.GiftPersonalDataGroup
import io.axoniq.framework.extension.dataprotection.api.{DeepPersonalData, PersonalData, SerializedPersonalData}
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.Person

import java.math.BigDecimal
import java.time.LocalDate
import scala.beans.BeanProperty

/**
 * Read model representation of a gift card's current state.
 *
 */
class GiftCardSummary {

  @BeanProperty
  var giftCardId: String = _

  @BeanProperty
  var remainingValue: BigDecimal = _

  @BeanProperty
  var initialValue: BigDecimal = _

  @BeanProperty
  @PersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
  var username: String = _

  @BeanProperty
  @DeepPersonalData
  var owner: Person = _

  @BeanProperty
  @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
  var dateOfBirth: LocalDate = _

  @BeanProperty
  @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
  var randomNumber: Integer = _

  def this(
    giftCardId: String,
    remainingValue: BigDecimal,
    initialValue: BigDecimal,
    username: String,
    owner: Person,
    dateOfBirth: LocalDate,
    randomNumber: Integer
  ) = {
    this()
    this.giftCardId = giftCardId
    this.remainingValue = remainingValue
    this.initialValue = initialValue
    this.username = username
    this.owner = owner
    this.dateOfBirth = dateOfBirth
    this.randomNumber = randomNumber
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: GiftCardSummary => giftCardId == that.giftCardId
    case _ => false
  }

  override def hashCode(): Int = {
    if (giftCardId != null) giftCardId.hashCode else 0
  }

  override def toString: String = {
    s"GiftCardSummary(giftCardId='$giftCardId', remainingValue=$remainingValue, " +
      s"initialValue=$initialValue, username='$username', owner=$owner, " +
      s"dateOfBirth=$dateOfBirth, randomNumber=$randomNumber)"
  }
}
