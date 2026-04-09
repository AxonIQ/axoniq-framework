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
package io.axoniq.framework.dataprotection.fieldencryption

import io.axoniq.framework.dataprotection.api.DataSubjectId
import io.axoniq.framework.dataprotection.api.PersonalData
import java.util.*

class BasicKotlinTest : AbstractBasicTestSet<BasicKotlinTest.KtPersonRegisteredEvent>() {

    override fun createEvent(id: UUID, name: String, picture: ByteArray, city: String): KtPersonRegisteredEvent {
        return KtPersonRegisteredEvent(id, name, picture, city)
    }

    override fun getId(event: KtPersonRegisteredEvent): UUID {
        return event.id;
    }

    override fun getName(event: KtPersonRegisteredEvent): String {
        return event.name;
    }

    override fun getPicture(event: KtPersonRegisteredEvent): ByteArray {
        return event.picture;
    }

    override fun getCity(event: KtPersonRegisteredEvent): String {
        return event.city;
    }

    data class KtPersonRegisteredEvent(
            @field:DataSubjectId val id: UUID,
            @field:PersonalData val name: String,
            @field:PersonalData val picture: ByteArray,
            val city: String) {

        /* Our Kotlin class has an array - in those cases you need to override equals and hashcode. */

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as KtPersonRegisteredEvent

            if (id != other.id) return false
            if (name != other.name) return false
            if (!Arrays.equals(picture, other.picture)) return false
            if (city != other.city) return false

            return true
        }

        override fun hashCode(): Int {
            var result = id.hashCode()
            result = 31 * result + name.hashCode()
            result = 31 * result + Arrays.hashCode(picture)
            result = 31 * result + city.hashCode()
            return result
        }
    }

}

