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

import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.GiftPersonalDataGroup
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.Person
import io.axoniq.framework.extension.dataprotection.api.DeepPersonalData
import io.axoniq.framework.extension.dataprotection.api.PersonalData
import io.axoniq.framework.extension.dataprotection.api.SerializedPersonalData
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import java.math.BigDecimal
import java.time.LocalDate
import java.time.OffsetDateTime

/**
 * JPA entity representing a gift card in the query/read model.
 *
 */
@Entity
@Table(name = "gift_card", schema = "dataprotection")
class GiftCardEntity() {

    @Id
    var giftCardId: String? = null

    var remainingValue: BigDecimal? = null

    var initialValue: BigDecimal? = null

    @PersonalData(group = GiftPersonalDataGroup.GROUP_NAME)
    var username: String? = null

    // Owner person data - stored as JSON
    @DeepPersonalData
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    var owner: Person? = null

    @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    var dateOfBirth: LocalDate? = null

    @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    var randomNumber: Int? = null

    var creationDate: OffsetDateTime? = null

    var lastModifiedDate: OffsetDateTime? = null

    constructor(
        giftCardId: String,
        initialValue: BigDecimal,
        remainingValue: BigDecimal,
        username: String,
        owner: Person,
        dateOfBirth: LocalDate?,
        randomNumber: Int?,
        creationDate: OffsetDateTime
    ) : this() {
        this.giftCardId = giftCardId
        this.initialValue = initialValue
        this.remainingValue = remainingValue
        this.username = username
        this.owner = owner
        this.dateOfBirth = dateOfBirth
        this.randomNumber = randomNumber
        this.creationDate = creationDate
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is GiftCardEntity) return false
        return giftCardId == other.giftCardId
    }

    override fun hashCode(): Int {
        return giftCardId?.hashCode() ?: 0
    }

    override fun toString(): String {
        return "GiftCardEntity(" +
                "giftCardId='$giftCardId', " +
                "remainingValue=$remainingValue, " +
                "initialValue=$initialValue, " +
                "username='$username', " +
                "creationDate=$creationDate, " +
                "lastModifiedDate=$lastModifiedDate)"
    }
}
