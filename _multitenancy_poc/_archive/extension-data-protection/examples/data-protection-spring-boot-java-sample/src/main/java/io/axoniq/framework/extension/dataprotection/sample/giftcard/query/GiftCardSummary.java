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

package io.axoniq.framework.extension.dataprotection.sample.giftcard.query;

import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.GiftPersonalDataGroup;
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.Person;
import io.axoniq.framework.extension.dataprotection.api.DeepPersonalData;
import io.axoniq.framework.extension.dataprotection.api.PersonalData;
import io.axoniq.framework.extension.dataprotection.api.SerializedPersonalData;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Objects;

/**
 * Read model representation of a gift card's current state.
 *
 */
public class GiftCardSummary {

    private String giftCardId;

    private BigDecimal remainingValue;

    private BigDecimal initialValue;

    @PersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    private String username;

    @DeepPersonalData
    private Person owner;

    @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    private LocalDate dateOfBirth;

    @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    private Integer randomNumber;

    public GiftCardSummary() {
    }

    public GiftCardSummary(String giftCardId, BigDecimal remainingValue, BigDecimal initialValue,
                          String username, Person owner, LocalDate dateOfBirth, Integer randomNumber) {
        this.giftCardId = giftCardId;
        this.remainingValue = remainingValue;
        this.initialValue = initialValue;
        this.username = username;
        this.owner = owner;
        this.dateOfBirth = dateOfBirth;
        this.randomNumber = randomNumber;
    }

    public String getGiftCardId() {
        return giftCardId;
    }

    public void setGiftCardId(String giftCardId) {
        this.giftCardId = giftCardId;
    }

    public BigDecimal getRemainingValue() {
        return remainingValue;
    }

    public void setRemainingValue(BigDecimal remainingValue) {
        this.remainingValue = remainingValue;
    }

    public BigDecimal getInitialValue() {
        return initialValue;
    }

    public void setInitialValue(BigDecimal initialValue) {
        this.initialValue = initialValue;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Person getOwner() {
        return owner;
    }

    public void setOwner(Person owner) {
        this.owner = owner;
    }

    public LocalDate getDateOfBirth() {
        return dateOfBirth;
    }

    public void setDateOfBirth(LocalDate dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    public Integer getRandomNumber() {
        return randomNumber;
    }

    public void setRandomNumber(Integer randomNumber) {
        this.randomNumber = randomNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GiftCardSummary that = (GiftCardSummary) o;
        return Objects.equals(giftCardId, that.giftCardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(giftCardId);
    }

    @Override
    public String toString() {
        return "GiftCardSummary{" +
                "giftCardId='" + giftCardId + '\'' +
                ", remainingValue=" + remainingValue +
                ", initialValue=" + initialValue +
                ", username='" + username + '\'' +
                ", owner=" + owner +
                ", dateOfBirth=" + dateOfBirth +
                ", randomNumber=" + randomNumber +
                '}';
    }
}
