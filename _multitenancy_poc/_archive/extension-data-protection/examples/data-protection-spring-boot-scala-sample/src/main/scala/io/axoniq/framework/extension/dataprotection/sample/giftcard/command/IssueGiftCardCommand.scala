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

package io.axoniq.framework.extension.dataprotection.sample.giftcard.command

import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.Person
import org.axonframework.modelling.annotation.TargetEntityId

import java.math.BigDecimal
import java.time.LocalDate
import scala.annotation.meta.field
import scala.beans.BeanProperty

/**
 * Command for issuing a new gift card with an initial amount and personal data.
 *
 * This command represents the intention to create a new gift card in the system.
 * It contains the unique identifier for the gift card, the initial monetary amount,
 * and encrypted personal data fields.
 *
 * Business constraints:
 * - The gift card ID must be unique within the system
 * - The amount must be positive (validated by the aggregate)
 *
 * @param giftCardId the unique identifier for the gift card to be created
 * @param amount the initial monetary amount to load onto the gift card (must be positive)
 * @param username encrypted username
 * @param owner deep encrypted owner person data (name + address)
 * @param dateOfBirth serialized encrypted date of birth
 * @param randomNumber serialized encrypted random number
 */
case class IssueGiftCardCommand(
  @BeanProperty
  @(TargetEntityId @field)
  giftCardId: String,

  @BeanProperty
  amount: BigDecimal,

  @BeanProperty
  username: String,

  @BeanProperty
  owner: Person,

  @BeanProperty
  dateOfBirth: LocalDate,

  @BeanProperty
  randomNumber: Integer
)
