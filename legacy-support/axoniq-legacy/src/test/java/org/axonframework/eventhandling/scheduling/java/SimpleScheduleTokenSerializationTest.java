/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.eventhandling.scheduling.java;

import org.axonframework.conversion.Converter;
import org.axonframework.deadline.AxonFramework4;
import org.axonframework.deadline.AxonFramework4.Flavor;
import org.axonframework.eventhandling.scheduling.ScheduleToken;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests conversion capabilities of {@link SimpleScheduleToken}, including tokens that Axon Framework 4.13.2 wrote into
 * Saga state, for each serializer an Axon Framework 4 application could have used.
 *
 * @author JohT
 * @author Jakob Hatzl
 */
class SimpleScheduleTokenSerializationTest {

    private static final String AXON_FRAMEWORK_4_CLASS =
            "org.axonframework.eventhandling.scheduling.java.SimpleScheduleToken";
    private static final String TOKEN_ID = "28bda08d-2dd5-4420-98cb-75ca073446b4";

    private static AxonFramework4 axonFramework4;

    @BeforeAll
    static void openAxonFramework4() throws IOException {
        axonFramework4 = new AxonFramework4();
    }

    @AfterAll
    static void closeAxonFramework4() throws IOException {
        axonFramework4.close();
    }

    @ParameterizedTest
    @EnumSource(Flavor.class)
    void tokenShouldBeSerializable(Flavor flavor) {
        // given
        Converter converter = flavor.axonFramework5Converter();
        SimpleScheduleToken tokenToTest = new SimpleScheduleToken(TOKEN_ID);

        // when
        ScheduleToken result = converter.convert(converter.convert(tokenToTest, String.class), ScheduleToken.class);

        // then
        assertThat(result).isEqualTo(tokenToTest);
    }

    @Test
    void keepsTheClassNameOfAxonFramework4() {
        assertThat(SimpleScheduleToken.class.getName()).isEqualTo(AXON_FRAMEWORK_4_CLASS);
    }

    @Nested
    class AxonFramework4Compatibility {

        @ParameterizedTest
        @EnumSource(Flavor.class)
        void tokenWrittenByAxonFramework4IsRead(Flavor flavor) {
            // given
            Object axonFramework4Token = axonFramework4.create(AXON_FRAMEWORK_4_CLASS, TOKEN_ID);
            Object stored = axonFramework4.serialize(flavor.axonFramework4Serializer(axonFramework4),
                                                     axonFramework4Token,
                                                     String.class);

            // when
            ScheduleToken result = flavor.axonFramework5Converter().convert(stored, ScheduleToken.class);

            // then
            assertThat(result).isEqualTo(new SimpleScheduleToken(TOKEN_ID));
        }

        @ParameterizedTest
        @EnumSource(Flavor.class)
        void tokenWrittenByAxonFramework5IsReadByAxonFramework4(Flavor flavor) {
            // given
            String stored = flavor.axonFramework5Converter().convert(new SimpleScheduleToken(TOKEN_ID), String.class);

            // when
            Object result = axonFramework4.deserialize(flavor.axonFramework4Serializer(axonFramework4),
                                                       stored,
                                                       AXON_FRAMEWORK_4_CLASS);

            // then
            assertThat(result.getClass()).isEqualTo(axonFramework4.type(AXON_FRAMEWORK_4_CLASS));
            assertThat(axonFramework4.call(result, "getTokenId")).isEqualTo(TOKEN_ID);
        }

        @Test
        void javaSerializedTokenOfAxonFramework4IsRead() {
            // given
            byte[] stored = AxonFramework4.javaSerialize(axonFramework4.create(AXON_FRAMEWORK_4_CLASS, TOKEN_ID));

            // when
            Object result = AxonFramework4.javaDeserializeAsAxonFramework5(stored);

            // then
            assertThat(result).isEqualTo(new SimpleScheduleToken(TOKEN_ID));
        }

        @Test
        void javaSerializedTokenOfAxonFramework5IsReadByAxonFramework4() {
            // given
            byte[] stored = AxonFramework4.javaSerialize(new SimpleScheduleToken(TOKEN_ID));

            // when
            Object result = axonFramework4.javaDeserializeAsAxonFramework4(stored);

            // then
            assertThat(result.getClass()).isEqualTo(axonFramework4.type(AXON_FRAMEWORK_4_CLASS));
            assertThat(axonFramework4.call(result, "getTokenId")).isEqualTo(TOKEN_ID);
        }
    }
}
