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

package org.axonframework.extension.tracing.opentelemetry;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

class MetadataContextGetterTest {

    private final EventMessage message = asEventMessage("MyEvent")
            .andMetadata(Collections.singletonMap("myKeyOne", "myValueTwo"))
            .andMetadata(Collections.singletonMap("MyKeyTwo", "2"));

    @Test
    void shouldReceiveMetadataKeysFromMessage() {
        List<String> keys = StreamSupport.stream(MetadataContextGetter.INSTANCE.keys(message).spliterator(), false)
                                         .collect(Collectors.toList());
        assertTrue(keys.contains("myKeyOne"));
        assertTrue(keys.contains("MyKeyTwo"));
        assertFalse(keys.contains("MyKeyThree"));
    }

    @Test
    void shouldGetItemFromMessage() {
        assertEquals("myValueTwo", MetadataContextGetter.INSTANCE.get(message, "myKeyOne"));
    }

    @Test
    void shouldGetNullFromNullMessage() {
        assertNull(MetadataContextGetter.INSTANCE.get(null, "myKeyOne"));
    }

    private static <P> EventMessage asEventMessage(P event) {
        return new GenericEventMessage(
                new GenericMessage(new MessageType(event.getClass()), event),
                () -> GenericEventMessage.clock.instant()
        );
    }
}
