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

import org.axonframework.messaging.tracing.SpanAttributesProvider;
import org.junit.jupiter.api.*;

@Disabled("TODO #3594")
class AggregateIdentifierSpanAttributesProviderTest {

    private final SpanAttributesProvider provider = new AggregateIdentifierSpanAttributesProvider();

//    @Test
//    void domainEventMessage() {
//        Message message = new GenericDomainEventMessage(
//                "MyType", "1729872981", 1, new MessageType("event"), "payload"
//        );
//
//        Map<String, String> map = provider.provideForMessage(message);
//        assertEquals(1, map.size());
//        assertEquals("1729872981", map.get("axon_aggregate_identifier"));
//    }
//
//    @Test
//    void genericEventMessage() {
//        Message message = new GenericEventMessage(new MessageType("event"), "payload");
//
//        Map<String, String> map = provider.provideForMessage(message);
//        assertEquals(0, map.size());
//    }
}
