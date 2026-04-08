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

package org.axonframework.extension.spring.messaging;


import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link DefaultEventMessageConverter}.
 *
 * @author Reda.Housni-Alaoui
 */
class DefaultEventMessageConverterTest {

    private final EventMessageConverter eventMessageConverter = new DefaultEventMessageConverter();

    @Test
    void givenGenericEventMessageWhenConvertingTwiceThenResultingEventShouldBeTheSame() {
        String id = UUID.randomUUID().toString();
        MessageType name = new MessageType("event");
        EventPayload payload = new EventPayload("hello");
        Map<String, String> metadata = new HashMap<>();
        metadata.put("number", "100");
        metadata.put("string", "world");
        Instant instant = Instant.EPOCH;

        EventMessage axonMessage =
                new GenericEventMessage(id, name, payload, metadata, instant);

        EventMessage convertedAxonMessage = eventMessageConverter.convertFromInboundMessage(
                eventMessageConverter.convertToOutboundMessage(axonMessage)
        );

        assertEquals(instant, convertedAxonMessage.timestamp());
        assertEquals("100", convertedAxonMessage.metadata().get("number"));
        assertEquals("world", convertedAxonMessage.metadata().get("string"));
        assertEquals("hello", convertedAxonMessage.payloadAs(EventPayload.class).name);
        assertEquals(id, convertedAxonMessage.identifier());
    }

    private record EventPayload(String name) {

    }
}
