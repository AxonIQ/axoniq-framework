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

package org.axonframework.messaging.commandhandling;

import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.junit.jupiter.api.*;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link MetadataRoutingStrategy}.
 *
 * @author Steven van Beelen
 */
class MetadataRoutingStrategyTest {

    private static final String METADATA_KEY = "some-metadata-key";
    private static final MessageType TEST_NAME = new MessageType("command");

    private MetadataRoutingStrategy testSubject;

    private final RoutingStrategy fallbackRoutingStrategy = mock(RoutingStrategy.class);

    @BeforeEach
    void setUp() {
        testSubject = new MetadataRoutingStrategy(METADATA_KEY);
    }

    @Test
    void resolvesRoutingKeyFromMetadata() {
        String expectedRoutingKey = "some-routing-key";

        Metadata testMetadata = Metadata.from(Collections.singletonMap(METADATA_KEY, expectedRoutingKey));
        CommandMessage testCommand = new GenericCommandMessage(TEST_NAME, "some-payload", testMetadata);

        assertEquals(expectedRoutingKey, testSubject.getRoutingKey(testCommand));
        verifyNoInteractions(fallbackRoutingStrategy);
    }

    @Test
    void returnsNullOnUnresolvedMetadataKey() {
        Metadata noMetadata = Metadata.emptyInstance();
        CommandMessage testCommand = new GenericCommandMessage(TEST_NAME, "some-payload", noMetadata);

        assertNull(testSubject.getRoutingKey(testCommand));
    }
}