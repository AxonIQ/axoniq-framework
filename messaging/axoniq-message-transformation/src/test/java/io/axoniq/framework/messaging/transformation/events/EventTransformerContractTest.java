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
import io.axoniq.framework.messaging.transformation.MessageTransformer;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.eventOf;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract test for the {@link EventTransformer} SPI shape: lambda construction,
 * return-type covariance, and the 0 / 1 / many output cardinality.
 */
final class EventTransformerContractTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");

    @Test
    void canBeImplementedAsLambdaSinceItIsFunctionalInterface() {
        EventTransformer identityTransformer = (message, context) -> MessageStream.just(message);

        MessageStream<? extends EventMessage> result = identityTransformer.transform(eventOf(V1, "payload"), null);

        assertThat(result).isNotNull();
        assertThat(identityTransformer).isInstanceOf(MessageTransformer.class);
    }

    @Test
    void returnsSingleMessageStreamForOneToOneTransformer() {
        EventTransformer v1ToV2Transformer = (message, context) -> MessageStream.just(eventOf(V2, "v2-payload"));

        List<EventMessage> outputs = collectMessages(v1ToV2Transformer.transform(eventOf(V1, "v1-payload"), null));

        assertThat(outputs).hasSize(1);
        assertThat(outputs.getFirst().type()).isEqualTo(V2);
    }

    @Test
    void canReturnEmptyStreamForDropTransformer() {
        EventTransformer droppingTransformer = (message, context) -> MessageStream.empty();

        List<EventMessage> outputs = collectMessages(droppingTransformer.transform(eventOf(V1, "payload"), null));

        assertThat(outputs).isEmpty();
    }

    @Test
    void canReturnMultiElementStreamForSplitTransformer() {
        EventTransformer splittingTransformer = (message, context) -> MessageStream.fromIterable(List.of(
                eventOf(V2, "out-1"),
                eventOf(V2, "out-2")
        ));

        List<EventMessage> outputs = collectMessages(splittingTransformer.transform(eventOf(V1, "payload"), null));

        assertThat(outputs).hasSize(2);
        assertThat(outputs).extracting(EventMessage::payload).containsExactly("out-1", "out-2");
    }

    @Test
    void toleratesNullProcessingContext() {
        EventTransformer identityTransformer = (message, context) -> MessageStream.just(message);

        assertThat(collectMessages(identityTransformer.transform(eventOf(V1, "payload"), null))).hasSize(1);
    }

    @Test
    void chainForwardsTheActiveProcessingContextToTheUsersMapper() {
        AtomicReference<@Nullable ProcessingContext> seenContext = new AtomicReference<>();
        EventTransformer v1ToV2Transformer = EventTransformation.from(V1).to(V2)
                .transform(JsonNode.class, (in, ctx) -> {
                    seenContext.set(ctx);
                    return in.deepCopy();
                });
        EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();
        EventMessage input = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());
        var realContext = StubProcessingContext.forMessage(input);

        collectMessages(chain.transform(
                MessageStream.fromIterable(List.of(input)), realContext, EventStreamTestUtils.neverInvokedConverter()));

        assertThat(seenContext.get()).isSameAs(realContext);
    }
}
