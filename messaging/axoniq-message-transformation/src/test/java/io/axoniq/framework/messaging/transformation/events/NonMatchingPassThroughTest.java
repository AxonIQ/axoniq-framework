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
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Events whose {@link MessageType} matches no registered transformation pass through the
 * chain unchanged, and no payload conversion happens on the non-matching path.
 */
final class NonMatchingPassThroughTest {

    private static final MessageType REGISTERED = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType UNREGISTERED = new MessageType("com.example.SystemHeartbeat", "1.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();

    @Test
    void eventWhoseTypeMatchesNoRegisteredTransformerPassesThroughUnchanged() {
        EventTransformer registeredTransformer = EventTransformation.from(REGISTERED)
                                                                    .to(new MessageType("com.example.CourseCreated", "2.0.0"))
                                                                    .transform(JsonNode.class, (in, ctx) -> in);
        EventTransformerChain chain = EventTransformerChain.builder().register(registeredTransformer).build();
        EventMessage unregisteredEvent = new GenericEventMessage(UNREGISTERED, "heartbeat-payload");

        List<EventMessage> observed = collectMessages(chain.transform(MessageStream.fromIterable(List.of(unregisteredEvent)), null, CONVERTER));

        assertThat(observed).hasSize(1);
        assertThat(observed.getFirst()).isSameAs(unregisteredEvent);
        assertThat(observed.getFirst().type()).isEqualTo(UNREGISTERED);
    }

    @Test
    void noPayloadConversionHappensForNonMatchingEvents() {
        AtomicInteger mapperInvocations = new AtomicInteger();
        EventTransformer registeredTransformer = EventTransformation.from(REGISTERED)
                                                                    .to(new MessageType("com.example.CourseCreated", "2.0.0"))
                                                                    .transform(JsonNode.class, (in, ctx) -> {
                                                                        mapperInvocations.incrementAndGet();
                                                                        return in;
                                                                    });
        EventTransformerChain chain = EventTransformerChain.builder().register(registeredTransformer).build();
        EventMessage unregisteredEvent = new GenericEventMessage(UNREGISTERED, "heartbeat-payload");

        collectMessages(chain.transform(MessageStream.fromIterable(List.of(unregisteredEvent)), null, CONVERTER));

        assertThat(mapperInvocations.get()).isZero();
    }
}
