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
package io.axoniq.framework.dataprotection.fieldencryption;

import io.axoniq.framework.dataprotection.api.DataSubjectId;
import io.axoniq.framework.dataprotection.api.PersonalData;

import java.util.UUID;

/**
 * Test class to verify that Java records work with field-level encryption.
 * Java records have final fields which require special handling using Unsafe API.
 */
public class JavaRecordTest extends AbstractBasicTestSet<JavaRecordTest.PersonRegisteredRecord> {

    @Override
    public PersonRegisteredRecord createEvent(UUID id, String name, byte[] picture, String city) {
        return new PersonRegisteredRecord(id, name, picture, city);
    }

    @Override
    public UUID getId(PersonRegisteredRecord event) {
        return event.id();
    }

    @Override
    public String getName(PersonRegisteredRecord event) {
        return event.name();
    }

    @Override
    public byte[] getPicture(PersonRegisteredRecord event) {
        return event.picture();
    }

    @Override
    public String getCity(PersonRegisteredRecord event) {
        return event.city();
    }

    /**
     * Java record with @PersonalData annotated fields.
     * Records are immutable, so encryption/decryption creates new instances.
     */
    public record PersonRegisteredRecord(
            @DataSubjectId UUID id,
            @PersonalData String name,
            @PersonalData byte[] picture,
            String city
    ) {
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof PersonRegisteredRecord other)) return false;
            return java.util.Objects.equals(id, other.id)
                    && java.util.Objects.equals(name, other.name)
                    && java.util.Arrays.equals(picture, other.picture)
                    && java.util.Objects.equals(city, other.city);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(id, name, java.util.Arrays.hashCode(picture), city);
        }
    }

}
