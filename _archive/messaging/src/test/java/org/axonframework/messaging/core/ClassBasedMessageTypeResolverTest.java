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

package org.axonframework.messaging.core;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link ClassBasedMessageTypeResolver}.
 *
 * @author Mateusz Nowak
 */
class ClassBasedMessageTypeResolverTest {

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Nested
    class Resolve {

        @Test
        void shouldUseDefaultVersionWhenCreatedWithDefaultConstructor() {
            // given
            QualifiedName expectedName = new QualifiedName(String.class);
            ClassBasedMessageTypeResolver resolver = new ClassBasedMessageTypeResolver();

            // when
            MessageType result = resolver.resolve("Test").get();

            // then
            assertEquals(expectedName, result.qualifiedName());
            assertEquals(MessageType.DEFAULT_VERSION, result.version());
        }

        @Test
        void shouldUseCustomVersionWhenProvidedInConstructor() {
            // given
            QualifiedName expectedName = new QualifiedName(Integer.class);
            String customVersion = "1.0.0";
            ClassBasedMessageTypeResolver resolver = new ClassBasedMessageTypeResolver(customVersion);

            // when
            MessageType result = resolver.resolve(42).get();

            // then
            assertEquals(expectedName, result.qualifiedName());
            assertEquals(customVersion, result.version());
        }

        @Test
        void shouldResolvePrimitiveTypeToCorrespondingWrapperClass() {
            // given
            QualifiedName expectedName = new QualifiedName(Integer.class);
            ClassBasedMessageTypeResolver resolver = new ClassBasedMessageTypeResolver();

            // when
            MessageType result = resolver.resolve(42).get();

            // then
            assertEquals(expectedName, result.qualifiedName());
            assertEquals(MessageType.DEFAULT_VERSION, result.version());
        }

        @Test
        void shouldResolveCustomClassWithProvidedVersion() {
            // given
            QualifiedName expectedName = new QualifiedName(TestPayload.class);
            String customVersion = "custom-rev";
            ClassBasedMessageTypeResolver resolver = new ClassBasedMessageTypeResolver(customVersion);

            // when
            MessageType result = resolver.resolve(new TestPayload()).get();

            // then
            assertEquals(expectedName, result.qualifiedName());
            assertEquals(customVersion, result.version());
        }

        @Test
        void shouldKeepMessageTypeIfPayloadIsAMessageAndDoNotApplyResolverVersion() {
            // given
            String customVersion = "custom-rev";
            ClassBasedMessageTypeResolver resolver = new ClassBasedMessageTypeResolver(customVersion);

            // when
            MessageType type = new MessageType("TestPayload");
            var payload = new GenericMessage(type, new TestPayload());
            MessageType result = resolver.resolve(payload).get();

            // then
            assertEquals(type, result);
            assertEquals(MessageType.DEFAULT_VERSION, result.version());
        }
    }


    @Nested
    class ResolveOrThrow {

        @Test
        void shouldUseDefaultVersionWhenCreatedWithDefaultConstructor() {
            // given
            QualifiedName expectedName = new QualifiedName(String.class);
            ClassBasedMessageTypeResolver resolver = new ClassBasedMessageTypeResolver();

            // when
            MessageType result = resolver.resolveOrThrow("Test");

            // then
            assertEquals(expectedName, result.qualifiedName());
            assertEquals(MessageType.DEFAULT_VERSION, result.version());
        }

        @Test
        void shouldUseCustomVersionWhenProvidedInConstructor() {
            // given
            QualifiedName expectedName = new QualifiedName(Integer.class);
            String customVersion = "1.0.0";
            ClassBasedMessageTypeResolver resolver = new ClassBasedMessageTypeResolver(customVersion);

            // when
            MessageType result = resolver.resolveOrThrow(42);

            // then
            assertEquals(expectedName, result.qualifiedName());
            assertEquals(customVersion, result.version());
        }

        @Test
        void shouldResolvePrimitiveTypeToCorrespondingWrapperClass() {
            // given
            QualifiedName expectedName = new QualifiedName(Integer.class);
            ClassBasedMessageTypeResolver resolver = new ClassBasedMessageTypeResolver();

            // when
            MessageType result = resolver.resolveOrThrow(42);

            // then
            assertEquals(expectedName, result.qualifiedName());
            assertEquals(MessageType.DEFAULT_VERSION, result.version());
        }

        @Test
        void shouldResolveCustomClassWithProvidedVersion() {
            // given
            QualifiedName expectedName = new QualifiedName(TestPayload.class);
            String customVersion = "custom-rev";
            ClassBasedMessageTypeResolver resolver = new ClassBasedMessageTypeResolver(customVersion);

            // when
            MessageType result = resolver.resolveOrThrow(new TestPayload());

            // then
            assertEquals(expectedName, result.qualifiedName());
            assertEquals(customVersion, result.version());
        }

        @Test
        void shouldKeepMessageTypeIfPayloadIsAMessageAndDoNotApplyResolverVersion() {
            // given
            String customVersion = "custom-rev";
            ClassBasedMessageTypeResolver resolver = new ClassBasedMessageTypeResolver(customVersion);

            // when
            MessageType type = new MessageType("TestPayload");
            var payload = new GenericMessage(type, new TestPayload());
            MessageType result = resolver.resolveOrThrow(payload);

            // then
            assertEquals(type, result);
            assertEquals(MessageType.DEFAULT_VERSION, result.version());
        }
    }

    private static class TestPayload {

    }
}