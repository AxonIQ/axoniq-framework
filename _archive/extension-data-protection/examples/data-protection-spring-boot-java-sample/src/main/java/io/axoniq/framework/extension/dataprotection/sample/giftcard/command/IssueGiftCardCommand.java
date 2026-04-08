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

import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.Person;
import org.axonframework.modelling.annotation.TargetEntityId;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Command for issuing a new gift card with an initial amount and personal data.
 *
 * <p>This command represents the intention to create a new gift card in the system.
 * It contains the unique identifier for the gift card, the initial monetary amount,
 * and encrypted personal data fields.</p>
 *
 * <p>Business constraints:</p>
 * <ul>
 *   <li>The gift card ID must be unique within the system</li>
 *   <li>The amount must be positive (validated by the aggregate)</li>
 * </ul>
 *
 * @param giftCardId the unique identifier for the gift card to be created
 * @param amount the initial monetary amount to load onto the gift card (must be positive)
 * @param username encrypted username
 * @param owner deep encrypted owner person data (name + address)
 * @param dateOfBirth serialized encrypted date of birth
 * @param randomNumber serialized encrypted random number
 */
public record IssueGiftCardCommand(
        @TargetEntityId String giftCardId,
        BigDecimal amount,
        String username,
        Person owner,
        LocalDate dateOfBirth,
        Integer randomNumber
) {
}
