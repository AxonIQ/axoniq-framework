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

package org.axonframework.eventsourcing.eventstore;

import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventstreaming.Tag;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link MetadataBasedTagResolver}.
 *
 * @author Mateusz Nowak
 */
class MetadataBasedTagResolverTest {

    private static final String METADATA_KEY = "testKey";
    private static final GenericEventMessage TEST_EVENT = new GenericEventMessage(
            new MessageType("test", "event", "0.0.1"),
            "payload",
            Map.of(METADATA_KEY, "testValue")
    );

    @Test
    void resolveReturnsExpectedTagWhenMetadataKeyExists() {
        // given
        MetadataBasedTagResolver testSubject = new MetadataBasedTagResolver(METADATA_KEY);

        // when
        Set<Tag> result = testSubject.resolve(TEST_EVENT);

        // then
        assertEquals(1, result.size());
        assertTrue(result.contains(new Tag(METADATA_KEY, "testValue")));
    }

    @Test
    void resolveReturnsEmptySetWhenMetadataKeyDoesNotExist() {
        // given
        MetadataBasedTagResolver testSubject = new MetadataBasedTagResolver("nonExistentKey");

        // when
        Set<Tag> result = testSubject.resolve(TEST_EVENT);

        // then
        assertTrue(result.isEmpty());
    }

    @Test
    void constructorThrowsNullPointerExceptionForNullMetadataKey() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new MetadataBasedTagResolver(null));
    }
}