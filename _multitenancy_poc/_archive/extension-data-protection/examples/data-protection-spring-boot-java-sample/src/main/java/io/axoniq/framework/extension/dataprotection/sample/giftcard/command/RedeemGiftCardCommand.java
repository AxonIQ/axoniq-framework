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

package io.axoniq.framework.extension.dataprotection.sample.giftcard.command;

import org.axonframework.modelling.annotation.TargetEntityId;

import java.math.BigDecimal;

/**
 * Command for redeeming an amount from an existing gift card.
 *
 * <p>This command represents the intention to redeem (withdraw) a specific amount from
 * an existing gift card. The redemption reduces the remaining balance on the gift card
 * and can be performed multiple times until the balance is exhausted.</p>
 *
 * <p>Business constraints:</p>
 * <ul>
 *   <li>The gift card must exist in the system</li>
 *   <li>The redemption amount must be positive (validated by the aggregate)</li>
 *   <li>The redemption amount cannot exceed the current remaining balance (validated by the aggregate)</li>
 * </ul>
 *
 * <p>Use cases:</p>
 * <ul>
 *   <li>Customer purchasing items using gift card balance</li>
 *   <li>Partial redemptions allowing multiple transactions</li>
 *   <li>Administrative redemptions or refunds</li>
 * </ul>
 *
 * @param giftCardId the unique identifier of the existing gift card to redeem from
 * @param amount     the monetary amount to redeem (must be positive and not exceed remaining balance)
 */
public record RedeemGiftCardCommand(@TargetEntityId String giftCardId, BigDecimal amount) {
}
