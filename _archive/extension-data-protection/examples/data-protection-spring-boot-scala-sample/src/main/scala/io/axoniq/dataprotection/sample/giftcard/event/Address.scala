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

import io.axoniq.dataprotection.sample.giftcard.GiftPersonalDataGroup
import io.axoniq.framework.dataprotection.api.PersonalData

import scala.annotation.meta.field

/**
 * Address case class containing personal data fields.
 *
 */
case class Address(
  @(PersonalData @field)(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "removed")
  line1: String,

  @(PersonalData @field)(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "removed")
  line2: String,

  country: String
)
