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
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code from(List)} matches an event whose type equals any of the listed identities, applying one mapper to all of
 * them; a version that is not listed is left untouched.
 */
final class EventTransformerChainMultiVersionFromTest {

    private static final MessageType V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.CourseCreated", "2.0.0");
    private static final MessageType V3 = new MessageType("com.example.CourseCreated", "3.0.0");
    private static final MessageType TARGET = new MessageType("com.example.CourseCreated", "9.0.0");
    private static final MessageType UNLISTED = new MessageType("com.example.CourseCreated", "4.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();
    private static final MessageTypeResolver RESOLVER = alwaysEmptyMessageTypeResolver();

    @Test
    void everyListedVersionIsTransformedBySingleRegistration() {
        // given one registration covering versions 1, 2 and 3
        EventTransformerChain chain = chainWith(
                EventTransformation.from(List.of(V1, V2, V3))
                                   .to(TARGET)
                                   .transform(JsonNode.class, (in, ctx) -> in));

        // when events of each listed version are transformed
        List<EventMessage> outputs = transform(chain, V1, V2, V3);

        // then all of them are mapped to the single target
        assertThat(outputs).extracting(EventMessage::type).containsExactly(TARGET, TARGET, TARGET);
    }

    @Test
    void aVersionNotInTheListPassesThroughUnchanged() {
        // given one registration covering versions 1, 2 and 3
        EventTransformerChain chain = chainWith(
                EventTransformation.from(List.of(V1, V2, V3))
                                   .to(TARGET)
                                   .transform(JsonNode.class, (in, ctx) -> in));

        // when an unlisted version is transformed
        EventMessage unlisted = new GenericEventMessage(UNLISTED, JsonNodeFactory.instance.objectNode());
        List<EventMessage> outputs =
                collectMessages(chain.transform(MessageStream.fromIterable(List.of(unlisted)), null, CONVERTER, RESOLVER));

        // then it is returned unchanged
        assertThat(outputs).hasSize(1);
        assertThat(outputs.getFirst()).isSameAs(unlisted);
    }

    @Nested
    final class InputValidation {

        @Test
        void nullListIsRejected() {
            //noinspection DataFlowIssue
            assertThatThrownBy(() -> EventTransformation.from((List<MessageType>) null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("sources");
        }

        @Test
        void nullElementIsRejected() {
            List<MessageType> withNull = new ArrayList<>();
            withNull.add(V1);
            withNull.add(null);

            //noinspection DataFlowIssue
            assertThatThrownBy(() -> EventTransformation.from(withNull))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("sources element");
        }

        @Test
        void emptyListIsRejected() {
            assertThatThrownBy(() -> EventTransformation.from(List.<MessageType>of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("at least one source");
        }
    }

    private static EventTransformerChain chainWith(EventTransformation transformation) {
        return EventTransformerChain.builder().register(transformation).build();
    }

    private static List<EventMessage> transform(EventTransformerChain chain, MessageType... types) {
        List<EventMessage> events = Arrays.stream(types)
                                          .map(type -> (EventMessage) new GenericEventMessage(
                                                  type, JsonNodeFactory.instance.objectNode()))
                                          .toList();
        return collectMessages(chain.transform(MessageStream.fromIterable(events), null, CONVERTER, RESOLVER));
    }
}
