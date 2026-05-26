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

package io.axoniq.framework.tracing.attributes;

import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AttributeProvidersTest {

    private final EventMessage event = EventTestUtils.asEventMessage("the-payload");

    @Nested
    class MessageId {

        @Test
        void addsTheMessageIdentifier() {
            // when
            var attributes = new MessageIdSpanAttributesProvider().provideForMessage(event, null);

            // then
            assertThat(attributes).containsEntry(MessageIdSpanAttributesProvider.MESSAGE_ID, event.identifier());
        }
    }

    @Nested
    class MessageName {

        @Test
        void addsTheQualifiedName() {
            // when
            var attributes = new MessageNameSpanAttributesProvider().provideForMessage(event, null);

            // then
            assertThat(attributes)
                    .containsEntry(MessageNameSpanAttributesProvider.MESSAGE_NAME, event.type().qualifiedName().name());
        }
    }

    @Nested
    class MessageType {

        @Test
        void classifiesAnEventMessageAsEvent() {
            // when
            var attributes = new MessageTypeSpanAttributesProvider().provideForMessage(event, null);

            // then
            assertThat(attributes).containsEntry(MessageTypeSpanAttributesProvider.MESSAGE_TYPE, "EVENT");
        }
    }

    @Nested
    class PayloadType {

        @Test
        void addsThePayloadClassName() {
            // when
            var attributes = new PayloadTypeSpanAttributesProvider().provideForMessage(event, null);

            // then
            assertThat(attributes)
                    .containsEntry(PayloadTypeSpanAttributesProvider.PAYLOAD_TYPE, String.class.getName());
        }
    }

    @Nested
    class MetadataAttributes {

        @Test
        void addsAllMetadataByDefault() {
            // given
            Message withMetadata = event.andMetadata(java.util.Map.of("tenant", "acme"));

            // when
            var attributes = new MetadataSpanAttributesProvider().provideForMessage(withMetadata, null);

            // then
            assertThat(attributes)
                    .containsEntry(MetadataSpanAttributesProvider.METADATA_PREFIX + "tenant", "acme");
        }

        @Test
        void addsOnlyAllowlistedKeysWhenConfigured() {
            // given
            Message withMetadata = event.andMetadata(java.util.Map.of("tenant", "acme", "secret", "hidden"));

            // when
            var attributes = new MetadataSpanAttributesProvider("tenant").provideForMessage(withMetadata, null);

            // then
            assertThat(attributes)
                    .containsEntry(MetadataSpanAttributesProvider.METADATA_PREFIX + "tenant", "acme")
                    .doesNotContainKey(MetadataSpanAttributesProvider.METADATA_PREFIX + "secret");
        }
    }

    @Nested
    class AggregateIdentifier {

        @Test
        void addsAggregateIdentifierFromLegacyResource() {
            // given
            StubProcessingContext context = new StubProcessingContext();
            context.putResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "aggregate-42");

            // when
            var attributes = new AggregateIdentifierSpanAttributesProvider().provideForMessage(event, context);

            // then
            assertThat(attributes)
                    .containsEntry(AggregateIdentifierSpanAttributesProvider.AGGREGATE_IDENTIFIER, "aggregate-42");
        }

        @Test
        void emptyWhenNoContext() {
            // when
            var attributes = new AggregateIdentifierSpanAttributesProvider().provideForMessage(event, null);

            // then
            assertThat(attributes).isEmpty();
        }

        @Test
        void emptyWhenResourceAbsent() {
            // when
            var attributes =
                    new AggregateIdentifierSpanAttributesProvider().provideForMessage(event, new StubProcessingContext());

            // then
            assertThat(attributes).isEmpty();
        }
    }
}
