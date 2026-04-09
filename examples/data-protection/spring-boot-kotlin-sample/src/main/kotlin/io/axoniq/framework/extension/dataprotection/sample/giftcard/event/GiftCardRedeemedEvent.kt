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

import org.axonframework.eventsourcing.annotation.EventTag
import java.math.BigDecimal

/**
 * Event representing the successful redemption of an amount from a gift card.
 *
 * This event is emitted when a specified amount has been successfully redeemed
 * (withdrawn) from an existing gift card, reducing its remaining balance. The event
 * represents an immutable fact that has occurred in the system.
 *
 * Event characteristics:
 * - Immutable - once emitted, represents a permanent transaction
 * - Auditable - provides complete trail of gift card usage
 * - Replay-safe - multiple replays produce consistent state
 * - Business-relevant - represents actual value transfer
 *
 * Business implications:
 * - The gift card balance is reduced by the redeemed amount
 * - Multiple redemptions can occur until balance reaches zero
 * - Each redemption creates a permanent audit record
 *
 * @param giftCardId the unique identifier of the gift card from which the amount was redeemed
 * @param amount the monetary amount that was redeemed (always positive, never exceeds available balance)
 */
data class GiftCardRedeemedEvent(
    @EventTag
    val giftCardId: String,
    val amount: BigDecimal
)
