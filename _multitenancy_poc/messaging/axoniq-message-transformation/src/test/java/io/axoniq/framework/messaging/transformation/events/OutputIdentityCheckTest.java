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
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.resolverFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Resolver-permitting output-identity check for 1:1 transformations with a payload mapper.
 * The framework attempts to resolve the output payload's {@link MessageType} via
 * {@code MessageTypeResolver.resolve(Class<?>)} and compares it to the declared {@code to}.
 * Cases covered:
 * <ul>
 *     <li>typed POJO mismatch -> raises {@link ChainConfigurationException};</li>
 *     <li>typed POJO match -> no exception;</li>
 *     <li>structural output ({@code JsonNode} / {@code Map} / raw bytes etc.) is
 *     short-circuited inside the chain via {@link StructuralPayloadTypes}, so the check
 *     is skipped even when the supplied resolver would otherwise resolve the carrier
 *     class to a mismatching {@link MessageType};</li>
 *     <li>diagnostic exception includes the stream position when the storage engine
 *     attached a {@link TrackingToken} to the entry context, and omits it gracefully when
 *     no token is available (entity-load / DCB-source paths).</li>
 * </ul>
 */
final class OutputIdentityCheckTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();

    @Nested
    final class PojoOutput {

        @Test
        void mismatchBetweenDeclaredToAndResolvedPojoMessageTypeRaises() {
            EventTransformer wrongTypeProducingTransformer = EventTransformer.from(V1)
                                                                                .to(V2)
                                                                                .transform(JsonNode.class, (in, ctx) -> new WrongTypePojo());
            EventTransformerChain chain = EventTransformerChain.builder().register(wrongTypeProducingTransformer).build();
            EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());
            // WrongTypePojo resolves to a MessageType other than the declared V2.
            MessageTypeResolver resolver = resolverFor(WrongTypePojo.class,
                                                       new MessageType(WrongTypePojo.class.getName(), "1.0.0"));
            MessageStream<EventMessage> transformed = chain.transform(
                    MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER, resolver);

            assertThatThrownBy(() -> collectMessages(transformed))
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(ChainConfigurationException.class)
                    .hasMessageContaining(V2.toString())
                    .hasMessageContaining(WrongTypePojo.class.getName());
        }

        @Test
        void matchBetweenDeclaredToAndResolvedPojoMessageTypePassesSilently() {
            EventTransformer matchingPojoTransformer = EventTransformer.from(V1)
                                                                          .to(V2)
                                                                          .transform(JsonNode.class, (in, ctx) -> new SamplePojoV2());
            EventTransformerChain chain = EventTransformerChain.builder().register(matchingPojoTransformer).build();
            EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());
            // SamplePojoV2 resolves to exactly the declared V2.
            MessageTypeResolver resolver = resolverFor(SamplePojoV2.class, V2);

            List<EventMessage> outputs = collectMessages(
                    chain.transform(MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER, resolver));

            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(V2);
        }
    }

    @Nested
    final class UntypedOutput {

        /**
         * A would-mismatch resolver: it would resolve the structural carrier to a non-matching
         * {@link MessageType} and therefore TRIP the identity check if it were consulted. The
         * chain's structural-aware guard must short-circuit before this resolver is reached,
         * so its presence here proves the guard works regardless of what the user supplies.
         */
        private final MessageTypeResolver wouldMismatchOnStructuralOutput =
                payloadClass -> Optional.of(new MessageType(payloadClass.getName(), "0.0.1"));

        @Test
        void jsonNodeOutputSkipsIdentityCheckSilently() {
            EventTransformer jsonNodeProducingTransformer = EventTransformer.from(V1)
                                                                               .to(V2)
                                                                               .transform(JsonNode.class, (in, ctx) -> JsonNodeFactory.instance.objectNode());
            EventTransformerChain chain = EventTransformerChain.builder().register(jsonNodeProducingTransformer).build();
            EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER, wouldMismatchOnStructuralOutput));

            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(V2);
        }

        @Test
        void mapOutputSkipsIdentityCheckSilently() {
            EventTransformer mapProducingTransformer = EventTransformer.from(V1)
                                                                          .to(V2)
                                                                          .transform(JsonNode.class, (in, ctx) -> {
                                                                              Map<String, Object> result = new HashMap<>();
                                                                              result.put("key", "value");
                                                                              return result;
                                                                          });
            EventTransformerChain chain = EventTransformerChain.builder().register(mapProducingTransformer).build();
            EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER, wouldMismatchOnStructuralOutput));

            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(V2);
        }
    }

    @Nested
    final class StreamPositionInErrors {

        @Test
        void mismatchErrorIncludesStreamPositionWhenTrackingTokenIsOnTheEntryContext() {
            EventTransformer wrongTypeProducingTransformer = EventTransformer.from(V1)
                                                                                .to(V2)
                                                                                .transform(JsonNode.class, (in, ctx) -> new WrongTypePojo());
            EventTransformerChain chain = EventTransformerChain.builder().register(wrongTypeProducingTransformer).build();
            EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());
            MessageTypeResolver resolver = resolverFor(WrongTypePojo.class,
                                                       new MessageType(WrongTypePojo.class.getName(), "1.0.0"));
            // Simulate the storage engine's behavior: attach a TrackingToken to the stream
            // entry's context. The chain MUST surface this position in identity-mismatch
            // diagnostics so operators can locate the failing event in their stream.
            TrackingToken positionToken = new GlobalSequenceTrackingToken(42L);
            MessageStream<EventMessage> messageStream = MessageStream.fromIterable(
                    List.of(storedV1Event), event -> TrackingToken.addToContext(Context.empty(), positionToken));
            MessageStream<EventMessage> transformed = chain.transform(messageStream, null, CONVERTER, resolver);

            assertThatThrownBy(() -> collectMessages(transformed))
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(ChainConfigurationException.class)
                    .hasMessageContaining("position=")
                    .hasMessageContaining(positionToken.toString());
        }

        @Test
        void mismatchErrorOmitsStreamPositionGracefullyWhenNoTrackingTokenIsAvailable() {
            EventTransformer wrongTypeProducingTransformer = EventTransformer.from(V1)
                                                                                .to(V2)
                                                                                .transform(JsonNode.class, (in, ctx) -> new WrongTypePojo());
            EventTransformerChain chain = EventTransformerChain.builder().register(wrongTypeProducingTransformer).build();
            EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());
            MessageTypeResolver resolver = resolverFor(WrongTypePojo.class,
                                                       new MessageType(WrongTypePojo.class.getName(), "1.0.0"));

            // Plain fromIterable -> no TrackingToken on the entry context (entity-load / DCB
            // shape). The error still contains the event identity but no position.
            MessageStream<EventMessage> transformed = chain.transform(
                    MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER, resolver);

            assertThatThrownBy(() -> collectMessages(transformed))
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(ChainConfigurationException.class)
                    .hasMessageContaining(storedV1Event.identifier())
                    .hasMessageNotContainingAny("position=");
        }
    }

    @Nested
    final class IdentityCheckBypass {

        @Test
        void transformerConstructedWithSkipIdentityCheckTrueDoesNotInvokeTheResolver() {
            // Phase 4 rename builds a DefaultEventTransformer with skipIdentityCheck=true, so the
            // framework-supplied output identity (the declared 'to') is not double-checked
            // against a possibly annotated source POJO's class. Wire the constructor here to
            // prove the flag actually bypasses the check.
            EventTransformer renameLikeTransformer = new DefaultEventTransformer<>(
                    new FromMatcher.Concrete(V1),
                    V2,
                    JsonNode.class,
                    JsonNode.class,
                    (in, ctx) -> new WrongTypePojo(),
                    true);
            EventTransformerChain chain = EventTransformerChain.builder().register(renameLikeTransformer).build();
            EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());
            // A resolver that WOULD flag a mismatch, but the chain must not consult it.
            MessageTypeResolver wouldMismatchResolver = payloadClass -> {
                throw new AssertionError("MessageTypeResolver.resolve was unexpectedly invoked despite skipIdentityCheck=true");
            };

            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER, wouldMismatchResolver));

            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type())
                    .as("framework-set output identity must be the declared 'to' regardless of mapper output class")
                    .isEqualTo(V2);
            assertThat(outputs.getFirst().payload()).isInstanceOf(WrongTypePojo.class);
        }
    }

    /** Helper POJO, whose class differs from any registered MessageType. */
    private static final class WrongTypePojo {
    }

    /** Helper POJO that would resolve to V2 once {@code @Event}-annotated. */
    private static final class SamplePojoV2 {
    }
}
