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
import io.axoniq.framework.messaging.transformation.ChainConfigurationException;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The chain enforces a per-event iteration bound. Default is
 * {@link EventTransformerChain#DEFAULT_MAX_ITERATIONS_PER_EVENT}; deep-history domains can
 * raise it via {@link EventTransformerChain.Builder#maxIterationsPerEvent(int)}.
 */
final class EventTransformerChainSafetyBoundTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");
    private static final MessageType V3 = new MessageType("com.example.Sample", "3.0.0");
    private static final MessageType TERMINAL = new MessageType("com.example.Terminal", "1.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();
    private static final MessageTypeResolver RESOLVER = alwaysEmptyMessageTypeResolver();

    @Test
    void exceedingTheConfiguredBoundRaisesChainConfigurationExceptionNamingTheOverride() {
        AtomicInteger totalMapperInvocations = new AtomicInteger();
        EventTransformation v1ToV2 = EventTransformation.from(V1).to(V2)
                                                     .transform(JsonNode.class, (in, ctx) -> {
                                                         totalMapperInvocations.incrementAndGet();
                                                         return in;
                                                     });
        EventTransformation v2ToV1 = EventTransformation.from(V2).to(V1)
                                                     .transform(JsonNode.class, (in, ctx) -> {
                                                         totalMapperInvocations.incrementAndGet();
                                                         return in;
                                                     });
        EventTransformerChain cyclingChain = EventTransformerChain.builder()
                                                                  .maxIterationsPerEvent(3)
                                                                  .register(v1ToV2)
                                                                  .register(v2ToV1)
                                                                  .build();
        EventMessage event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());
        MessageStream<EventMessage> outputStream = cyclingChain.transform(
                MessageStream.fromIterable(List.of(event)), null, CONVERTER, RESOLVER);

        assertThatThrownBy(() -> collectMessages(outputStream))
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(ChainConfigurationException.class)
                .hasMessageContaining("exceeded 3 iterations")
                .hasMessageContaining("maxIterationsPerEvent");
        assertThat(totalMapperInvocations.get())
                .as("the chain must apply exactly maxIterationsPerEvent transformations before bailing")
                .isEqualTo(3);
    }

    @Test
    void aChainReachingAFixedPointOnTheLastAllowedIterationTerminatesCleanly() {
        // A linear chain of exactly two hops (V1 -> V2 -> V3, with V3 matching nothing) with the bound set to
        // exactly two. The last transformation applies on the final allowed iteration. The fixed point is then
        // reached without exceeding the bound. Pins that hitting the bound exactly is not an error, only exceeding
        // it is: the recursive check must let the last in-bound hop through.
        AtomicInteger totalMapperInvocations = new AtomicInteger();
        EventTransformation v1ToV2 = EventTransformation.from(V1).to(V2)
                                                        .transform(JsonNode.class, (in, ctx) -> {
                                                            totalMapperInvocations.incrementAndGet();
                                                            return in;
                                                        });
        EventTransformation v2ToV3 = EventTransformation.from(V2).to(V3)
                                                        .transform(JsonNode.class, (in, ctx) -> {
                                                            totalMapperInvocations.incrementAndGet();
                                                            return in;
                                                        });
        EventTransformerChain chain = EventTransformerChain.builder()
                                                           .maxIterationsPerEvent(2)
                                                           .register(v1ToV2)
                                                           .register(v2ToV3)
                                                           .build();
        EventMessage event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

        List<EventMessage> outputs = collectMessages(chain.transform(
                MessageStream.fromIterable(List.of(event)), null, CONVERTER, RESOLVER));

        assertThat(outputs).singleElement()
                           .satisfies(output -> assertThat(output.type()).isEqualTo(V3));
        assertThat(totalMapperInvocations.get())
                .as("both hops must run once each, reaching the fixed point exactly at the bound")
                .isEqualTo(2);
    }

    @Test
    void aSplitWhoseOutputReEntersTheChainTripsTheBound() {
        // A self-matching split: it emits an event of its own source type, which re-enters the chain and splits
        // again, plus a terminal output. This expands without a fixed point, so it must trip the per-event bound.
        AtomicInteger totalSplitInvocations = new AtomicInteger();
        EventTransformation selfSplit = EventTransformation.split(V1, String.class)
                                                           .producing(V1, payload -> {
                                                               totalSplitInvocations.incrementAndGet();
                                                               return payload;
                                                           })
                                                           .producing(TERMINAL, payload -> payload)
                                                           .build();
        EventTransformerChain chain = EventTransformerChain.builder()
                                                           .maxIterationsPerEvent(3)
                                                           .register(selfSplit)
                                                           .build();
        EventMessage event = new GenericEventMessage(V1, "payload");
        MessageStream<EventMessage> outputStream = chain.transform(
                MessageStream.fromIterable(List.of(event)), null, CONVERTER, RESOLVER);

        assertThatThrownBy(() -> collectMessages(outputStream))
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(ChainConfigurationException.class)
                .hasMessageContaining("exceeded 3 iterations")
                .hasMessageContaining("maxIterationsPerEvent");
        assertThat(totalSplitInvocations.get())
                .as("the self-producing output re-enters up to the bound, then trips")
                .isEqualTo(3);
    }

    @Test
    void builderAcceptsMaxIterationsOfOneAsTheSmallestValidValue() {
        // Boundary case: max=1 is the minimum allowed value; the validation rejects only 0
        // and negatives. Pins the inclusive lower bound.
        EventTransformerChain.Builder builder = EventTransformerChain.builder();

        assertThatCode(() -> builder.maxIterationsPerEvent(1)).doesNotThrowAnyException();
    }

    @Test
    void builderRejectsNonPositiveMaxIterations() {
        EventTransformerChain.Builder zeroBoundBuilder = EventTransformerChain.builder();
        EventTransformerChain.Builder negativeBoundBuilder = EventTransformerChain.builder();

        assertThatThrownBy(() -> zeroBoundBuilder.maxIterationsPerEvent(0))
                .isInstanceOf(AxonConfigurationException.class)
                .hasMessageContaining("maxIterationsPerEvent must be strictly positive");

        assertThatThrownBy(() -> negativeBoundBuilder.maxIterationsPerEvent(-5))
                .isInstanceOf(AxonConfigurationException.class)
                .hasMessageContaining("maxIterationsPerEvent must be strictly positive");
    }
}
