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

package org.axonframework.messaging.core.sequencing;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link MetadataSequencingPolicy}.
 *
 * @author Lucas Campos
 */
class MetadataSequencingPolicyTest {

    @Test
    void propertyShouldReadCorrectValue() {
        final MetadataSequencingPolicy metadataPolicy = new MetadataSequencingPolicy("metadataKey");

        EventMessage testEvent = EventTestUtils.asEventMessage("42").withMetadata(Map.of("metadataKey", "metadataValue"));

        assertThat(metadataPolicy.sequenceIdentifierFor(testEvent, new StubProcessingContext())).contains("metadataValue");
    }

    @Test
    void shouldReturnEmptyIfMetaDataDoesNotContainsTheKey() {
        final MetadataSequencingPolicy metadataPolicy = new MetadataSequencingPolicy("metadataKey");

        assertThat(metadataPolicy.sequenceIdentifierFor(EventTestUtils.asEventMessage("42"), new StubProcessingContext())).isEmpty();
    }

    @Test
    void nullMetadataKeyShouldThrowException() {
        assertThrows(AxonConfigurationException.class,
                     () -> new MetadataSequencingPolicy(null));
    }

    @Test
    void blankMetadataKeyShouldThrowException() {
        assertThrows(AxonConfigurationException.class,
                     () -> new MetadataSequencingPolicy("   "));
    }
}
