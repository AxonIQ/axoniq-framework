/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code from(Predicate<MessageType>)} overload of {@code EventTransformation} matches events whose
 * {@link MessageType} satisfies the predicate. Enables semver / regex / range matching without forcing users to
 * register one transformation per specific version.
 */
final class EventTransformerChainPredicateBasedMatchingTest {

    private static final MessageType V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType V1_PATCH = new MessageType("com.example.CourseCreated", "1.0.1");
    private static final MessageType V2 = new MessageType("com.example.CourseCreated", "2.0.0");
    private static final MessageType V3 = new MessageType("com.example.CourseCreated", "3.0.0");
    private static final MessageType UNRELATED = new MessageType("com.example.SystemHeartbeat", "1.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();
    private static final MessageTypeResolver RESOLVER = alwaysEmptyMessageTypeResolver();

    @Test
    void everyEventWhoseTypeSatisfiesThePredicateGetsTransformed() {
        EventTransformation oneToThreeRangeTransformation = EventTransformation.from(
                mt -> "com.example.CourseCreated".equals(mt.qualifiedName().name()) && mt.version().startsWith("1.")
        ).to(V3).transform(JsonNode.class, (in, ctx) -> in);
        EventTransformerChain chain = EventTransformerChain.builder().register(oneToThreeRangeTransformation).build();

        EventMessage v1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());
        EventMessage v1PatchEvent = new GenericEventMessage(V1_PATCH, JsonNodeFactory.instance.objectNode());
        EventMessage v2Event = new GenericEventMessage(V2, JsonNodeFactory.instance.objectNode());
        EventMessage unrelatedEvent = new GenericEventMessage(UNRELATED, JsonNodeFactory.instance.objectNode());

        List<EventMessage> outputs = collectMessages(chain.transform(MessageStream.fromIterable(
                List.of(v1Event, v1PatchEvent, v2Event, unrelatedEvent)), null, CONVERTER, RESOLVER));

        assertThat(outputs).extracting(EventMessage::type)
                           .containsExactly(V3, V3, V2, UNRELATED);
    }

    @Test
    void eventWhoseTypeFailsThePredicatePassesThroughUnchanged() {
        EventTransformation onlyHeartbeatsTransformation = EventTransformation.from(mt -> mt.qualifiedName().name()
                                                                                            .equals(UNRELATED.qualifiedName()
                                                                                                             .name()))
                                                                              .to(V3)
                                                                              .transform(JsonNode.class,
                                                                                         (in, ctx) -> in);
        EventTransformerChain chain = EventTransformerChain.builder().register(onlyHeartbeatsTransformation).build();

        EventMessage v1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

        List<EventMessage> outputs = collectMessages(chain.transform(MessageStream.fromIterable(List.of(v1Event)),
                                                                     null,
                                                                     CONVERTER,
                                                                     RESOLVER));

        assertThat(outputs).hasSize(1);
        assertThat(outputs.getFirst()).isSameAs(v1Event);
    }
}
