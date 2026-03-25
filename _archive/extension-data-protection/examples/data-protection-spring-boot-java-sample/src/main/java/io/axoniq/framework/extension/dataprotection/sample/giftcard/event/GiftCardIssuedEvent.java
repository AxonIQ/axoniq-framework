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

package io.axoniq.framework.extension.dataprotection.sample.giftcard.event;

import io.axoniq.framework.extension.dataprotection.api.DataSubjectId;
import io.axoniq.framework.extension.dataprotection.api.DeepPersonalData;
import io.axoniq.framework.extension.dataprotection.api.PersonalData;
import io.axoniq.framework.extension.dataprotection.api.SerializedPersonalData;
import org.axonframework.eventsourcing.annotation.EventTag;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Event representing the successful issuance of a new gift card.
 *
 * <p>This event is emitted when a gift card has been successfully created in the system
 * with an initial monetary amount. It represents a fact that has occurred and cannot be
 * changed, following the event sourcing principle of immutable events.</p>
 *
 * <p>Event characteristics:</p>
 * <ul>
 *   <li>Immutable - once emitted, the event data cannot be changed</li>
 *   <li>Reproducible - replaying this event will recreate the same state</li>
 *   <li>Self-contained - contains all necessary information about the event</li>
 * </ul>
 *
 * @param giftCardId the unique identifier of the newly issued gift card
 * @param amount     the initial monetary amount loaded onto the gift card (always positive)
 */
public record GiftCardIssuedEvent(@EventTag
                                  @DataSubjectId(
                                          group = GiftPersonalDataGroup.GROUP_NAME,
                                          prefix = GiftPersonalDataGroup.GROUP_PREFIX
                                  )
                                  String giftCardId,

                                  BigDecimal amount,

                                  @PersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
                                  String username,

                                  @DeepPersonalData
                                  Person owner,

                                  @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
                                  LocalDate dateOfBirth,
                                  byte[] dateOfBirthEncrypted,

                                  @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
                                  int n,
                                  String nEncrypted) {

    /**
     * Convenience constructor for creating event with all user-provided data.
     * Encrypted field pairs will be automatically populated by the data protection framework.
     */
    public GiftCardIssuedEvent(String giftCardId, BigDecimal amount, String username, Person owner, LocalDate dateOfBirth, int randomNumber) {
        this(giftCardId,
                amount,
                username,
                owner,
                dateOfBirth,
                null,
                randomNumber,
                null);
    }

    /**
     * @deprecated Use the constructor with Person owner parameter instead
     */
    @Deprecated
    public GiftCardIssuedEvent(String giftCardId, BigDecimal amount, String username, LocalDate dateOfBirth, int n) {
        this(giftCardId,
                amount,
                username,
                new Person("Stefan", new Address("line1", "line2", "RS")),
                dateOfBirth,
                null,
                n,
                null);
    }
}
