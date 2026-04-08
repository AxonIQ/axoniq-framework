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

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class HierarchicalMessageTypeResolverTest {

    @Nested
    class Resolve {

        @Test
        void shouldUseFallbackWhenDelegateDoNotResolve() {
            // given
            var expectedType = new MessageType("test.type", "1.0.0");
            MessageTypeResolver delegate = (pt) -> Optional.empty();
            MessageTypeResolver fallback = (pt) -> Optional.of(expectedType);
            HierarchicalMessageTypeResolver resolver = new HierarchicalMessageTypeResolver(delegate, fallback);

            // when
            var result = resolver.resolve(String.class);

            // then
            assertTrue(result.isPresent());
            assertEquals(expectedType, result.get());
        }

        @Test
        void shouldUseDelegateWhenItSucceeds() {
            // given
            var expectedType = new MessageType("test.type", "1.0.0");
            MessageTypeResolver delegate = (pt) -> Optional.of(expectedType);
            MessageTypeResolver fallback = (pt) -> Optional.empty();
            var resolver = new HierarchicalMessageTypeResolver(delegate, fallback);

            // when
            var result = resolver.resolve(String.class);

            // then
            assertTrue(result.isPresent());
            assertEquals(expectedType, result.get());
        }
    }

    @Nested
    class ResolveOrThrow {

        @Test
        void shouldNotThrowIfFallbackResolves() {
            // given
            var expectedType = new MessageType("test.type", "1.0.0");
            MessageTypeResolver delegate = (pt) -> Optional.empty();
            MessageTypeResolver fallback = (pt) -> Optional.of(expectedType);
            HierarchicalMessageTypeResolver resolver = new HierarchicalMessageTypeResolver(delegate, fallback);

            // when
            var result = assertDoesNotThrow(() -> resolver.resolveOrThrow(String.class));

            // then
            assertEquals(expectedType, result);
        }

        @Test
        void shouldThrowIfFallbackDoNotResolve() {
            // given
            MessageTypeResolver delegate = (pt) -> Optional.empty();
            MessageTypeResolver fallback = (pt) -> Optional.empty();
            HierarchicalMessageTypeResolver resolver = new HierarchicalMessageTypeResolver(delegate, fallback);

            // when/then
            assertThrows(MessageTypeNotResolvedException.class, () -> resolver.resolveOrThrow(String.class));
        }
    }
}