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

package org.axonframework.modelling.entity.annotation;

import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link RoutingKeyUtils}.
 *
 * @author Mitchell Herrijgers
 */
class RoutingKeyUtilsTest {

    static class ComplexType {

    }

    static class MemberWithRoutingKey {

        @EntityMember(routingKey = "myKey")
        private final ComplexType someField = new ComplexType();
    }

    static class MemberWithEmptyRoutingKey {

        @EntityMember
        private final ComplexType someField = new ComplexType();
    }

    static class MemberWithoutEntityMember {

        private final ComplexType someField = new ComplexType();
    }

    @Test
    void getMessageRoutingKey_returnsKeyWhenPresent() throws NoSuchFieldException {
        Field field = MemberWithRoutingKey.class.getDeclaredField("someField");
        Optional<String> key = RoutingKeyUtils.getMessageRoutingKey(field);
        assertTrue(key.isPresent());
        assertEquals("myKey", key.get());
    }

    @Test
    void getMessageRoutingKey_returnsEmptyWhenNoAnnotation() throws NoSuchFieldException {
        Field field = MemberWithoutEntityMember.class.getDeclaredField("someField");
        Optional<String> key = RoutingKeyUtils.getMessageRoutingKey(field);
        assertFalse(key.isPresent());
    }

    @Test
    void getMessageRoutingKey_returnsEmptyWhenEmptyKey() throws NoSuchFieldException {
        Field field = MemberWithEmptyRoutingKey.class.getDeclaredField("someField");
        Optional<String> key = RoutingKeyUtils.getMessageRoutingKey(field);
        assertFalse(key.isPresent());
    }
}