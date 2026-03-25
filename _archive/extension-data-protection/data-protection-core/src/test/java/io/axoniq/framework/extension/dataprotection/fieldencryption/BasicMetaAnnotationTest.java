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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.UUID;

/**
 * Runs the AbstractBasicTestSet against a class annotated with custom annotations.
 */
public class BasicMetaAnnotationTest extends AbstractBasicTestSet<BasicMetaAnnotationTest.PersonRegisteredEvent> {

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


    @DataSubjectId(group = "myGroup")
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface MyDataSubjectId {
    }

    @PersonalData(group = "myGroup")
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface MyPersonalData {
    }

    @Data
    public static class PersonRegisteredEvent {

        @MyDataSubjectId
        private final UUID id;

        @MyPersonalData
        private final String name;

        @PersonalData(group = "myGroup")
        private final byte[] picture;

        private final String city;
    }

}
