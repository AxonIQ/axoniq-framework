/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.messaging.conversion;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.lang.reflect.Type;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.*;

/**
 * Test class validating the parts of the {@link StoredMessageConverter} that its subclasses decide on: the payload of a
 * type name that resolves to no class, and the exception type of a failure. The stored form itself is covered through
 * the deadline managers' {@code StoredDeadlineConverter}.
 *
 * @author Jakob Hatzl
 */
class StoredMessageConverterTest {

    private final CallerConverter testSubject = new CallerConverter(new JacksonConverter());

    @Nested
    class Payload {

        @Test
        void unknownTypeNameYieldsUnknownPayloadOfSubclass() {
            // when
            Object result = testSubject.payload("com.example.Removed", "1.0", "{}");

            // then
            assertThat(result).isEqualTo("unknown com.example.Removed 1.0 {}");
        }

        @Test
        void unknownTypeNameFailsWhenSubclassThrows() {
            // given
            StoredMessageConverter failingOnUnknownType = new CallerConverter(new JacksonConverter()) {
                @Override
                protected Object unknownPayload(String typeName, @Nullable String revision, @Nullable Object data) {
                    throw exception("unknown " + typeName, null);
                }
            };

            // when / then
            assertThatThrownBy(() -> failingOnUnknownType.payload("com.example.Removed", null, "{}"))
                    .isInstanceOf(CallerException.class)
                    .hasMessage("unknown com.example.Removed");
        }

        @Test
        void emptyTypeNameYieldsNoPayload() {
            // when
            Object result = testSubject.payload(StoredMessageConverter.EMPTY_TYPE, null, "null");

            // then
            assertThat(result).isNull();
        }

        @Test
        void knownTypeNameIsConverted() {
            // when
            Object result = testSubject.payload(String.class.getName(), null, "\"text\"");

            // then
            assertThat(result).isEqualTo("text");
        }
    }

    @Nested
    class Failures {

        @Test
        void invalidStoredStringFailsWithExceptionOfSubclass() {
            // when / then
            assertThatThrownBy(() -> testSubject.payload(String.class.getName(), null, "text"))
                    .isInstanceOf(CallerException.class);
        }

        @Test
        void storingStringWithConverterOfUnknownFormatFailsWithExceptionOfSubclass() {
            // given
            StoredMessageConverter unknownFormat = new CallerConverter(new UnknownFormatConverter());

            // when / then
            assertThatThrownBy(() -> unknownFormat.payloadToStored("text", String.class))
                    .isInstanceOf(CallerException.class)
                    .hasMessageContaining(String.class.getName());
        }

        @Test
        void storedDataConvertingToNullFailsWithExceptionOfSubclass() {
            // when / then
            assertThatThrownBy(() -> testSubject.fromStored("null", HashMap.class))
                    .isInstanceOf(CallerException.class);
        }
    }

    /**
     * A subclass describing the unknown payload in a string and failing with a {@link CallerException}.
     */
    private static class CallerConverter extends StoredMessageConverter {

        private CallerConverter(Converter converter) {
            super(converter);
        }

        @Override
        protected Object unknownPayload(String typeName, @Nullable String revision, @Nullable Object data) {
            return "unknown " + typeName + " " + revision + " " + data;
        }

        @Override
        protected RuntimeException exception(String message, @Nullable Throwable cause) {
            return new CallerException(message, cause);
        }
    }

    private static final class CallerException extends RuntimeException {

        private CallerException(String message, @Nullable Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * A converter that writes neither JSON nor XStream XML.
     */
    private static final class UnknownFormatConverter implements Converter {

        @Override
        @SuppressWarnings("unchecked")
        public <T> T convert(Object input, Type targetType) {
            return (T) ("unknown:" + input);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("format", "unknown");
        }
    }
}
