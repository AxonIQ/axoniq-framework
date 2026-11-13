/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.events;

import com.fasterxml.jackson.databind.JsonNode;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedMessageTypeResolver;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Events whose {@link MessageType} matches no registered transformation pass through the chain unchanged, and no
 * payload conversion happens on the non-matching path.
 */
final class EventTransformerChainNonMatchingPassThroughTest {

    private static final MessageType REGISTERED = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType UNREGISTERED = new MessageType("com.example.SystemHeartbeat", "1.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();
    private static final MessageTypeResolver RESOLVER = neverInvokedMessageTypeResolver();

    @Test
    void eventWhoseTypeMatchesNoRegisteredTransformationPassesThroughUnchanged() {
        EventTransformation registeredTransformation = EventTransformation.from(REGISTERED)
                                                                          .to(new MessageType(
                                                                                  "com.example.CourseCreated",
                                                                                  "2.0.0"))
                                                                          .transform(JsonNode.class, (in, ctx) -> in);
        EventTransformerChain chain = EventTransformerChain.builder().register(registeredTransformation).build();
        EventMessage unregisteredEvent = new GenericEventMessage(UNREGISTERED, "heartbeat-payload");

        List<EventMessage> observed = collectMessages(chain.transform(MessageStream.fromIterable(List.of(
                unregisteredEvent)), null, CONVERTER, RESOLVER));

        assertThat(observed).hasSize(1);
        assertThat(observed.getFirst()).isSameAs(unregisteredEvent);
        assertThat(observed.getFirst().type()).isEqualTo(UNREGISTERED);
    }

    @Test
    void noPayloadConversionHappensForNonMatchingEvents() {
        AtomicInteger mapperInvocations = new AtomicInteger();
        EventTransformation registeredTransformation = EventTransformation.from(REGISTERED)
                                                                          .to(new MessageType(
                                                                                  "com.example.CourseCreated",
                                                                                  "2.0.0"))
                                                                          .transform(JsonNode.class, (in, ctx) -> {
                                                                              mapperInvocations.incrementAndGet();
                                                                              return in;
                                                                          });
        EventTransformerChain chain = EventTransformerChain.builder().register(registeredTransformation).build();
        EventMessage unregisteredEvent = new GenericEventMessage(UNREGISTERED, "heartbeat-payload");

        collectMessages(chain.transform(MessageStream.fromIterable(List.of(unregisteredEvent)),
                                        null,
                                        CONVERTER,
                                        RESOLVER));

        assertThat(mapperInvocations.get()).isZero();
    }
}
