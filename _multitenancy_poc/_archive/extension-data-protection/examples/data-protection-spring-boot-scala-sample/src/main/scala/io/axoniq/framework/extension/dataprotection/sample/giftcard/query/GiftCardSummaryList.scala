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

import scala.beans.BeanProperty
import scala.jdk.CollectionConverters._

/**
 * Wrapper class for a collection of gift card summaries.
 *
 * @param giftCards the list of gift card summaries (never null, may be empty)
 */
case class GiftCardSummaryList(@BeanProperty giftCards: java.util.List[GiftCardSummary]) {
  def this(scalaList: List[GiftCardSummary]) = this(scalaList.asJava)
}
