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

package org.axonframework.messaging.tracing.attributes;

import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.tracing.SpanAttributesProvider;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageTypeSpanAttributesProviderTest {

    private final SpanAttributesProvider provider = new MessageTypeSpanAttributesProvider();

    @Test
    void correctTypeForQueryMessage() {
        Message genericQueryMessage = new GenericQueryMessage(
                new MessageType("myQueryName"), "MyQuery"
        );
        Map<String, String> map = provider.provideForMessage(genericQueryMessage);
        assertEquals(1, map.size());
        assertEquals("GenericQueryMessage", map.get("axon_message_type"));
    }

    @Test
    void correctTypeForCommandMessage() {
        Message genericQueryMessage =
                new GenericCommandMessage(new MessageType("command"), "payload");
        Map<String, String> map = provider.provideForMessage(genericQueryMessage);
        assertEquals(1, map.size());
        assertEquals("GenericCommandMessage", map.get("axon_message_type"));
    }
}
