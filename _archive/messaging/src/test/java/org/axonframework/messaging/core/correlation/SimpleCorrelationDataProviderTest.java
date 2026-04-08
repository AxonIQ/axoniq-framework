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

package org.axonframework.messaging.core.correlation;

import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.*;

class SimpleCorrelationDataProviderTest {

    @Test
    void resolveCorrelationData() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");
        metadata.put("key3", "value3");
        Message message =
                new GenericMessage(new MessageType("message"), "payload", metadata);

        assertEquals(singletonMap("key1", "value1"),
                     new SimpleCorrelationDataProvider("key1").correlationDataFor(message));

        final Map<String, ?> actual2 = new SimpleCorrelationDataProvider("key1", "key2", "noExist", null)
                .correlationDataFor(message);
        assertEquals("value1", actual2.get("key1"));
        assertEquals("value2", actual2.get("key2"));
    }
}
