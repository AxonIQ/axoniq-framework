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

import io.axoniq.framework.messaging.transformation.ChainConfigurationException;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.eventOf;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the chain rejects a rename (a {@code QualifiedName} change) while still allowing
 * a same-name version bump. Renaming is unsupported because the transformation runs at read
 * time, after the storage engine has filtered the stream by the stored (old) name, so the
 * events a rename targets are never surfaced. A concrete {@code from} is rejected at
 * registration; a predicate {@code from} only at read time, as its matched name is known
 * per-event.
 */
final class NameChangeRejectedTest {

    private static final MessageType SAMPLE_V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType SAMPLE_V2 = new MessageType("com.example.Sample", "2.0.0");
    private static final MessageType RENAMED = new MessageType("com.example.Renamed", "1.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();

    @Nested
    final class ConcreteFrom {

        @Test
        void differentQualifiedNameRejectedAtRegistration() {
            // given
            EventTransformer renameTransformer = EventTransformer.from(SAMPLE_V1)
                                                                    .to(RENAMED)
                                                                    .transform(String.class, (in, ctx) -> in);
            EventTransformerChain.Builder builder = EventTransformerChain.builder();

            // when / then
            assertThatThrownBy(() -> builder.register(renameTransformer))
                    .isInstanceOf(ChainConfigurationException.class)
                    .hasMessageContaining("renaming")
                    .hasMessageContaining(SAMPLE_V1.toString())
                    .hasMessageContaining(RENAMED.toString());
        }

        @Test
        void sameQualifiedNameVersionBumpApplied() {
            // given
            EventTransformer structuralUpcast = EventTransformer.from(SAMPLE_V1)
                                                                   .to(SAMPLE_V2)
                                                                   .transform(String.class, (in, ctx) -> in + "-upcasted");
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(structuralUpcast)
                                                               .build();
            EventMessage storedV1 = eventOf(SAMPLE_V1, "payload");

            // when
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedV1)), null, CONVERTER, alwaysEmptyMessageTypeResolver()));

            // then
            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(SAMPLE_V2);
            assertThat(outputs.getFirst().payload()).isEqualTo("payload-upcasted");
        }
    }

    @Nested
    final class PredicateFrom {

        @Test
        void differentQualifiedNameRejectedAtReadTime() {
            // given: registration is allowed because a predicate's matched source name is
            // not known statically; the rename is only detectable once an event matches.
            EventTransformer renameTransformer = EventTransformer.from(type -> type.equals(SAMPLE_V1))
                                                                    .to(RENAMED)
                                                                    .transform(String.class, (in, ctx) -> in);
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(renameTransformer)
                                                               .build();
            EventMessage storedV1 = eventOf(SAMPLE_V1, "payload");
            MessageStream<EventMessage> transformed = chain.transform(
                    MessageStream.fromIterable(List.of(storedV1)), null, CONVERTER, alwaysEmptyMessageTypeResolver());

            // when / then
            assertThatThrownBy(() -> collectMessages(transformed))
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(ChainConfigurationException.class)
                    .hasMessageContaining("renaming")
                    .hasMessageContaining(SAMPLE_V1.toString())
                    .hasMessageContaining(RENAMED.toString());
        }

        @Test
        void sameQualifiedNameVersionBumpApplied() {
            // given
            EventTransformer structuralUpcast = EventTransformer.from(type -> type.equals(SAMPLE_V1))
                                                                   .to(SAMPLE_V2)
                                                                   .transform(String.class, (in, ctx) -> in + "-upcasted");
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(structuralUpcast)
                                                               .build();
            EventMessage storedV1 = eventOf(SAMPLE_V1, "payload");

            // when
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedV1)), null, CONVERTER, alwaysEmptyMessageTypeResolver()));

            // then
            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(SAMPLE_V2);
            assertThat(outputs.getFirst().payload()).isEqualTo("payload-upcasted");
        }
    }
}
