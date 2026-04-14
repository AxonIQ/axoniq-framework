/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.messaging.eventhandling.deadletter.jpa;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GapAwareTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link EventMessageDeadLetterJpaConverter}.
 * <p>
 * Tracking tokens and aggregate data (only if legacy Aggregate approach is used: aggregate identifier, type, sequence
 * number) are stored and restored as {@link Context} resources. This test verifies that the converter correctly handles
 * these resources.
 */
class EventMessageDeadLetterJpaConverterTest {

    private final EventMessageDeadLetterJpaConverter converter = new EventMessageDeadLetterJpaConverter();
    private final JacksonConverter jacksonConverter = new JacksonConverter();
    private final EventConverter eventConverter = new DelegatingEventConverter(jacksonConverter);
    private final Converter genericConverter = jacksonConverter;
    private final ConverterTestEvent event = new ConverterTestEvent("myValue");
    private final Metadata metadata = Metadata.from(Collections.singletonMap("myMetadataKey", "myMetadataValue"));

    @Test
    void canConvertGenericEventMessageAndBackCorrectly() {
        EventMessage message = EventTestUtils.asEventMessage(event).andMetadata(metadata);
        Context context = Context.empty();
        testConversion(message, context);
    }

    @Test
    void canConvertEventMessageWithTrackingTokenInContext() {
        EventMessage message = EventTestUtils.asEventMessage(event).andMetadata(metadata);
        TrackingToken token = new GlobalSequenceTrackingToken(232323L);
        Context context = Context.empty()
                                 .withResource(TrackingToken.RESOURCE_KEY, token);

        testConversionWithContext(message, context);
    }

    @Test
    void canConvertEventMessageWithGapAwareTrackingTokenInContext() {
        EventMessage message = EventTestUtils.asEventMessage(event).andMetadata(metadata);
        TrackingToken token = new GapAwareTrackingToken(232323L, Arrays.asList(24L, 255L, 2225L));
        Context context = Context.empty()
                                 .withResource(TrackingToken.RESOURCE_KEY, token);

        testConversionWithContext(message, context);
    }

    @Test
    void canConvertEventMessageWithDomainInfoInContext() {
        EventMessage message = EventTestUtils.asEventMessage(event).andMetadata(metadata);
        Context context = Context.empty()
                                 .withResource(LegacyResources.AGGREGATE_TYPE_KEY, "MyAggregateType")
                                 .withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "aggregate-123")
                                 .withResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY, 42L);

        testConversionWithContext(message, context);
    }

    @Test
    void canConvertEventMessageWithTrackingTokenAndDomainInfoInContext() {
        EventMessage message = EventTestUtils.asEventMessage(event).andMetadata(metadata);
        TrackingToken token = new GlobalSequenceTrackingToken(999L);
        Context context = Context.empty()
                                 .withResource(TrackingToken.RESOURCE_KEY, token)
                                 .withResource(LegacyResources.AGGREGATE_TYPE_KEY, "OrderAggregate")
                                 .withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "order-456")
                                 .withResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY, 10L);

        testConversionWithContext(message, context);
    }

    private void testConversion(EventMessage message, Context context) {
        DeadLetterEventEntry deadLetterEventEntry = converter.convert(message,
                                                                      context,
                                                                      eventConverter,
                                                                      genericConverter);

        assertCorrectlyMapped(message, context, deadLetterEventEntry);

        MessageStream.Entry<EventMessage> restoredEntry =
                converter.convert(deadLetterEventEntry, eventConverter, genericConverter);
        assertCorrectlyRestored(message, restoredEntry.message());
    }

    private void testConversionWithContext(EventMessage message, Context context) {
        DeadLetterEventEntry deadLetterEventEntry = converter.convert(message,
                                                                      context,
                                                                      eventConverter,
                                                                      genericConverter);

        assertCorrectlyMapped(message, context, deadLetterEventEntry);

        MessageStream.Entry<EventMessage> restoredEntry =
                converter.convert(deadLetterEventEntry, eventConverter, genericConverter);

        assertCorrectlyRestored(message, restoredEntry.message());
        assertContextRestored(context, restoredEntry);
    }

    private void assertCorrectlyRestored(EventMessage expected, EventMessage actual) {
        assertThat(actual.identifier()).isEqualTo(expected.identifier());
        assertThat(actual.timestamp()).isEqualTo(expected.timestamp());
        assertThat(actual.type()).isEqualTo(expected.type());
        assertThat(actual.metadata()).isEqualTo(expected.metadata());

        // Payload is stored as raw bytes; deserialize to compare with the original
        Object deserializedPayload = eventConverter.convertPayload(actual, expected.payloadType());
        assertThat(deserializedPayload).isEqualTo(expected.payload());

        // In AF5, all restored messages are GenericEventMessage
        assertThat(actual).isInstanceOf(GenericEventMessage.class);
    }

    private void assertContextRestored(Context originalContext, Context restoredContext) {
        // Check tracking token restoration
        if (originalContext.containsResource(TrackingToken.RESOURCE_KEY)) {
            assertThat(restoredContext.containsResource(TrackingToken.RESOURCE_KEY)).isTrue();
            assertThat(restoredContext.getResource(TrackingToken.RESOURCE_KEY))
                    .isEqualTo(originalContext.getResource(TrackingToken.RESOURCE_KEY));
        }

        // Check domain info restoration
        if (originalContext.containsResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)) {
            assertThat(restoredContext.containsResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)).isTrue();
            assertThat(restoredContext.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY))
                    .isEqualTo(originalContext.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY));
        }
        if (originalContext.containsResource(LegacyResources.AGGREGATE_TYPE_KEY)) {
            assertThat(restoredContext.containsResource(LegacyResources.AGGREGATE_TYPE_KEY)).isTrue();
            assertThat(restoredContext.getResource(LegacyResources.AGGREGATE_TYPE_KEY))
                    .isEqualTo(originalContext.getResource(LegacyResources.AGGREGATE_TYPE_KEY));
        }
        if (originalContext.containsResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY)) {
            assertThat(restoredContext.containsResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY)).isTrue();
            assertThat(restoredContext.getResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY))
                    .isEqualTo(originalContext.getResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY));
        }
    }

    private void assertCorrectlyMapped(EventMessage eventMessage, Context context, DeadLetterEventEntry entry) {
        assertThat(entry.getIdentifier()).isEqualTo(eventMessage.identifier());
        assertThat(entry.getTimestamp()).isEqualTo(eventMessage.timestamp().toString());
        assertThat(entry.getType()).isEqualTo(eventMessage.type().toString());

        // Check tracking token storage from context
        if (context.containsResource(TrackingToken.RESOURCE_KEY)) {
            assertThat(entry.getToken()).isNotNull();
            assertThat(entry.getTokenType()).isNotNull();
            TrackingToken expectedToken = context.getResource(TrackingToken.RESOURCE_KEY);
            assertThat(entry.getTokenType()).isEqualTo(expectedToken.getClass().getName());
        } else {
            assertThat(entry.getToken()).isNull();
            assertThat(entry.getTokenType()).isNull();
        }

        // Check domain info storage from context
        if (context.containsResource(LegacyResources.AGGREGATE_TYPE_KEY)) {
            assertThat(entry.getAggregateType()).isEqualTo(context.getResource(LegacyResources.AGGREGATE_TYPE_KEY));
        } else {
            assertThat(entry.getAggregateType()).isNull();
        }
        if (context.containsResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)) {
            assertThat(entry.getAggregateIdentifier()).isEqualTo(context.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY));
        } else {
            assertThat(entry.getAggregateIdentifier()).isNull();
        }
        if (context.containsResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY)) {
            assertThat(entry.getAggregateSequenceNumber())
                    .isEqualTo(context.getResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY));
        } else {
            assertThat(entry.getAggregateSequenceNumber()).isNull();
        }
    }

    @Nested
    class ConvertWithNullContext {

        @Test
        void setsAllContextDependentFieldsToNull() {
            // given
            EventMessage message = EventTestUtils.asEventMessage(event).andMetadata(metadata);

            // when
            DeadLetterEventEntry entry = converter.convert(message, null, eventConverter, genericConverter);

            // then
            assertThat(entry.getIdentifier()).isEqualTo(message.identifier());
            assertThat(entry.getTimestamp()).isEqualTo(message.timestamp().toString());
            assertThat(entry.getType()).isEqualTo(message.type().toString());
            assertThat(entry.getToken()).isNull();
            assertThat(entry.getTokenType()).isNull();
            assertThat(entry.getAggregateType()).isNull();
            assertThat(entry.getAggregateIdentifier()).isNull();
            assertThat(entry.getAggregateSequenceNumber()).isNull();
        }

        @Test
        void roundTripConversionRestoresMessageWithoutContextResources() {
            // given
            EventMessage message = EventTestUtils.asEventMessage(event).andMetadata(metadata);

            // when
            DeadLetterEventEntry entry = converter.convert(message, null, eventConverter, genericConverter);
            MessageStream.Entry<EventMessage> restoredEntry =
                    converter.convert(entry, eventConverter, genericConverter);

            // then - message is correctly restored
            EventMessage restored = restoredEntry.message();
            assertThat(restored.identifier()).isEqualTo(message.identifier());
            assertThat(restored.timestamp()).isEqualTo(message.timestamp());
            assertThat(restored.type()).isEqualTo(message.type());
            assertThat(restored.metadata()).isEqualTo(message.metadata());

            Object deserializedPayload = eventConverter.convertPayload(restored, message.payloadType());
            assertThat(deserializedPayload).isEqualTo(message.payload());

            // then - no context resources are restored
            assertThat(restoredEntry.containsResource(TrackingToken.RESOURCE_KEY)).isFalse();
            assertThat(restoredEntry.containsResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)).isFalse();
            assertThat(restoredEntry.containsResource(LegacyResources.AGGREGATE_TYPE_KEY)).isFalse();
            assertThat(restoredEntry.containsResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY)).isFalse();
        }
    }

    @Nested
    class InlinePayloadConversion {

        @Test
        void convertAttachesConverterToMessage() {
            // given
            EventMessage message = EventTestUtils.asEventMessage(event).andMetadata(metadata);
            DeadLetterEventEntry exEntry = new DeadLetterEventEntry(
                    message.type().toString(),
                    message.identifier(),
                    message.timestamp().toString(),
                    eventConverter.convert(message.payload(), byte[].class),
                    eventConverter.convert(message.metadata(), byte[].class),
                    null,
                    null,
                    null,
                    null,
                    null
            );

            // when
            MessageStream.Entry<EventMessage> acEntry = converter.convert(exEntry, eventConverter, genericConverter);

            // then
            assertThat(acEntry.message().payloadType()).isEqualTo(byte[].class);
            assertThat(acEntry.message().payloadAs(ConverterTestEvent.class)).isEqualTo(event);
        }
    }

    @Event
    public static class ConverterTestEvent {

        private final String myProperty;

        @JsonCreator
        public ConverterTestEvent(@JsonProperty("myProperty") String myProperty) {
            this.myProperty = myProperty;
        }

        @SuppressWarnings("unused")
        public String getMyProperty() {
            return myProperty;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ConverterTestEvent that = (ConverterTestEvent) o;

            return Objects.equals(myProperty, that.myProperty);
        }

        @Override
        public int hashCode() {
            return myProperty != null ? myProperty.hashCode() : 0;
        }
    }

    // Suppressed since it's used for testing serialization error scenarios
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    static class SerializationErrorClass {

        String myValue;
    }
}
