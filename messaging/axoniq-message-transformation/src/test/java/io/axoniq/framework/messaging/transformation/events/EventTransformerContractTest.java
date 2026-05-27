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

import io.axoniq.framework.messaging.transformation.MessageTransformer;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract test for the {@link EventTransformer} SPI shape: lambda construction,
 * return-type covariance, and the 0 / 1 / many output cardinality.
 */
class EventTransformerContractTest {

    private static final MessageType TYPE_V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType TYPE_V2 = new MessageType("com.example.Sample", "2.0.0");

    @Test
    void can_be_implemented_as_a_lambda_since_it_is_a_functional_interface() {
        // given
        EventTransformer t = (message, context) -> MessageStream.just(message);

        // when
        MessageStream<? extends EventMessage> result = t.transform(eventOf(TYPE_V1, "payload"), null);

        // then
        assertThat(result).isNotNull();
        assertThat(t).isInstanceOf(MessageTransformer.class);
    }

    @Test
    void returns_a_single_message_stream_for_a_one_to_one_transformer() {
        // given
        EventTransformer t = (message, context) -> MessageStream.just(eventOf(TYPE_V2, "v2-payload"));

        // when
        List<EventMessage> out = drain(t.transform(eventOf(TYPE_V1, "v1-payload"), null));

        // then
        assertThat(out).hasSize(1);
        assertThat(out.get(0).type()).isEqualTo(TYPE_V2);
    }

    @Test
    void can_return_an_empty_stream_for_a_drop_transformer() {
        // given
        EventTransformer t = (message, context) -> MessageStream.empty();

        // when
        List<EventMessage> out = drain(t.transform(eventOf(TYPE_V1, "payload"), null));

        // then
        assertThat(out).isEmpty();
    }

    @Test
    void can_return_a_multi_element_stream_for_a_split_transformer() {
        // given
        EventTransformer t = (message, context) -> MessageStream.fromIterable(List.of(
                eventOf(TYPE_V2, "out-1"),
                eventOf(TYPE_V2, "out-2")
        ));

        // when
        List<EventMessage> out = drain(t.transform(eventOf(TYPE_V1, "payload"), null));

        // then
        assertThat(out).hasSize(2);
        assertThat(out).extracting(EventMessage::payload).containsExactly("out-1", "out-2");
    }

    @Test
    void tolerates_a_null_processing_context() {
        // given
        EventTransformer t = (message, context) -> MessageStream.just(message);

        // when / then -- no exception, context is nullable
        assertThat(drain(t.transform(eventOf(TYPE_V1, "payload"), null))).hasSize(1);
    }

    private static EventMessage eventOf(MessageType type, Object payload) {
        return new GenericEventMessage(type, payload);
    }

    private static List<EventMessage> drain(MessageStream<? extends EventMessage> stream) {
        return stream.<List<EventMessage>>reduce(new ArrayList<>(), (acc, entry) -> {
            acc.add(entry.message());
            return acc;
        }).join();
    }
}
