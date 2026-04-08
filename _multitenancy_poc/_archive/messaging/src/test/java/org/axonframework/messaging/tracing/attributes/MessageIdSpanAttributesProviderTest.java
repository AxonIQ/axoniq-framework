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

import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.tracing.SpanAttributesProvider;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageIdSpanAttributesProviderTest {

    private final SpanAttributesProvider provider = new MessageIdSpanAttributesProvider();

    @Test
    void extractsMessageIdentifier() {
        Message event = EventTestUtils.asEventMessage("Some event");
        Map<String, String> map = provider.provideForMessage(event);
        assertEquals(1, map.size());
        assertEquals(event.identifier(), map.get("axon_message_id"));
    }
}
