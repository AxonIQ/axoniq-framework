/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.messaging.transformation.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Events stored without an explicit version are treated as version {@code "0.0.1"}
 * (AF5 default) and match transformations registered for that version. No special
 * API is required from the user.
 */
final class LegacyUnversionedEventTest {

    private static final QualifiedName NAME = new QualifiedName("com.example.LegacyEvent");
    private static final MessageType DEFAULT_VERSION = new MessageType(NAME, "0.0.1");
    private static final MessageType V2 = new MessageType(NAME, "2.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();
    private static final MessageTypeResolver RESOLVER = alwaysEmptyMessageTypeResolver();

    @Test
    void unversionedEventMatchesTransformationRegisteredForDefaultVersion() {
        EventTransformer defaultVersionToV2Transformer = EventTransformer.from(DEFAULT_VERSION)
                                                                            .to(V2)
                                                                            .transform(JsonNode.class, (in, ctx) -> in);
        EventTransformerChain chain = EventTransformerChain.builder().register(defaultVersionToV2Transformer).build();
        EventMessage legacyUnversionedEvent = new GenericEventMessage(new MessageType(NAME), JsonNodeFactory.instance.objectNode());

        assertThat(legacyUnversionedEvent.type().version()).isEqualTo("0.0.1");

        List<EventMessage> outputs = collectMessages(chain.transform(MessageStream.fromIterable(List.of(legacyUnversionedEvent)), null, CONVERTER, RESOLVER));

        assertThat(outputs).hasSize(1);
        assertThat(outputs.getFirst().type()).isEqualTo(V2);
    }
}
