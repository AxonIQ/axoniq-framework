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
package io.axoniq.framework.extension.dataprotection.fieldencryption;

import io.axoniq.framework.extension.dataprotection.api.DataSubjectId;
import io.axoniq.framework.extension.dataprotection.api.PersonalData;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.UUID;

/**
 * Runs the AbstractBasicTestSet against a class derived from a parentclass. (Using Lombok for convenience.)
 */
public class BasicInheritanceTest extends AbstractBasicTestSet<BasicInheritanceTest.PersonRegisteredEvent> {

    @Override
    public PersonRegisteredEvent createEvent(UUID id, String name, byte[] picture, String city) {
        return new PersonRegisteredEvent(id, name, picture, city);
    }

    @Override
    public UUID getId(PersonRegisteredEvent event) {
        return event.getId();
    }

    @Override
    public String getName(PersonRegisteredEvent event) {
        return event.getName();
    }

    @Override
    public byte[] getPicture(PersonRegisteredEvent event) {
        return event.getPicture();
    }

    @Override
    public String getCity(PersonRegisteredEvent event) {
        return event.getCity();
    }

    @Data
    public static class PersonEvent {

        @DataSubjectId
        private final UUID id;

        @PersonalData
        private final String name;

    }

    @ToString(callSuper=true) @EqualsAndHashCode(callSuper=false) @Getter
    public static class PersonRegisteredEvent extends PersonEvent {

        @PersonalData
        private final byte[] picture;

        private final String city;

        public PersonRegisteredEvent(UUID id, String name, byte[] picture, String city) {
            super(id, name);
            this.picture = picture;
            this.city = city;
        }
    }

}
