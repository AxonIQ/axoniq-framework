/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline;

import com.thoughtworks.xstream.XStream;
import org.axonframework.conversion.ChainingContentTypeConverter;
import org.axonframework.conversion.DelegatingGeneralConverter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.conversion.xstream.XStreamConverter;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.modelling.command.AggregateScopeDescriptor;
import org.axonframework.modelling.saga.SagaScopeDescriptor;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link StoredDeadlineConverter}.
 *
 * @author Jakob Hatzl
 */
class StoredDeadlineConverterTest {

    private final StoredDeadlineConverter testSubject = new StoredDeadlineConverter(new JacksonConverter());

    @Nested
    class Payload {

        @Test
        void theEmptyTypeNameIsReadAsNoPayloadWithoutConvertingTheData() {
            // when
            Object payload = testSubject.payload(StoredDeadlineConverter.EMPTY_TYPE, null, bytes("null"));

            // then
            assertThat(payload).isNull();
        }

        @Test
        void noPayloadIsStoredUnderTheEmptyTypeName() {
            // when / then
            assertThat(StoredDeadlineConverter.typeNameOf(null)).isEqualTo(StoredDeadlineConverter.EMPTY_TYPE);
        }

        @Test
        void aPayloadIsStoredUnderItsClassNameAndConvertedBackIntoThatClass() {
            // given
            OuterPayload.NestedPayload original = new OuterPayload.NestedPayload("value", 3);

            // when
            String typeName = StoredDeadlineConverter.typeNameOf(original);
            byte[] stored = testSubject.toStored(original, byte[].class);
            Object payload = testSubject.payload(typeName, null, stored);

            // then
            assertThat(typeName).isEqualTo(OuterPayload.NestedPayload.class.getName());
            assertThat(payload).isEqualTo(original);
        }

        @Test
        void anUnresolvableTypeNameYieldsAnUnknownDeadlinePayloadHoldingTheStoredData() {
            // given
            byte[] stored = bytes("{\"field\":\"value\"}");

            // when
            Object payload = testSubject.payload("com.example.RemovedPayload", "1", stored);

            // then
            assertThat(payload).isInstanceOfSatisfying(UnknownDeadlinePayload.class, unknown -> {
                assertThat(unknown.typeName()).isEqualTo("com.example.RemovedPayload");
                assertThat(unknown.revision()).isEqualTo("1");
                assertThat(unknown.data()).isSameAs(stored);
            });
        }

        @Test
        void theMessageTypeIsDerivedFromThePayloadClass() {
            // when / then
            assertThat(StoredDeadlineConverter.messageTypeOf(new OuterPayload.NestedPayload("value", 3)).name())
                    .isEqualTo(StoredDeadlineConverter.messageTypeOf(new OuterPayload.NestedPayload("other", 4))
                                                      .name());
            assertThat(StoredDeadlineConverter.messageTypeOf(new UnknownDeadlinePayload("type", null, null)).name())
                    .contains(UnknownDeadlinePayload.class.getSimpleName());
        }
    }

    @Nested
    class StringAndBytesPayloads {

        @Test
        void aStringPayloadIsStoredAsAJsonStringWithAJsonConverter() {
            // when
            String stored = testSubject.payloadToStored("say \"hi\"", String.class);

            // then
            assertThat(stored).isEqualTo("\"say \\\"hi\\\"\"");
            assertThat(testSubject.payload(String.class.getName(), null, stored)).isEqualTo("say \"hi\"");
        }

        @Test
        void aBytesPayloadIsStoredAsABase64JsonStringWithAJsonConverter() {
            // when
            byte[] stored = testSubject.payloadToStored(new byte[]{1, 2, 3}, byte[].class);

            // then
            assertThat(new String(stored, StandardCharsets.UTF_8)).isEqualTo("\"AQID\"");
            assertThat(testSubject.payload(byte[].class.getName(), null, stored)).isEqualTo(new byte[]{1, 2, 3});
        }

        @Test
        @SuppressWarnings("removal")
        void aStringPayloadIsStoredAsAnXStreamElementWithAnXStreamConverter() {
            // given
            StoredDeadlineConverter xStreamSubject = new StoredDeadlineConverter(new XStreamConverter(new XStream()));

            // when
            String stored = xStreamSubject.payloadToStored("a < b & c", String.class);

            // then
            assertThat(stored).isEqualTo("<string>a &lt; b &amp; c</string>");
            assertThat(xStreamSubject.payload("string", null, stored)).isEqualTo("a < b & c");
        }

        @Test
        void storingAStringPayloadFailsWithAConverterWritingNeitherJsonNorXStreamXml() {
            // given
            StoredDeadlineConverter unknownFormat = new StoredDeadlineConverter(new ChainingContentTypeConverter());

            // when / then
            assertThatThrownBy(() -> unknownFormat.payloadToStored("text", String.class))
                    .isInstanceOf(DeadlineException.class)
                    .hasMessageContaining(String.class.getName());
        }

        @Test
        @SuppressWarnings("removal")
        void aBytesPayloadIsStoredAsAnXStreamElementWithAnXStreamConverter() {
            // given
            StoredDeadlineConverter xStreamSubject = new StoredDeadlineConverter(new XStreamConverter(new XStream()));

            // when
            String stored = xStreamSubject.payloadToStored(new byte[]{1, 2, 3}, String.class);

            // then
            assertThat(stored).isEqualTo("<byte-array>AQID</byte-array>");
            assertThat(xStreamSubject.payload("byte-array", null, stored)).isEqualTo(new byte[]{1, 2, 3});
        }

        @Test
        void readingAnIncompleteJsonStringFails() {
            // when / then
            assertThatThrownBy(() -> testSubject.payload(String.class.getName(), null, bytes("\"text\\\"")))
                    .isInstanceOf(DeadlineException.class);
        }

        @Test
        void readingAnXStreamElementOfAnotherTypeFails() {
            // when / then
            assertThatThrownBy(() -> testSubject.payload(String.class.getName(), null, bytes("<int>1</int>")))
                    .isInstanceOf(DeadlineException.class);
        }

        @Test
        void readingAStringPayloadStoredAsIsFails() {
            // when / then
            assertThatThrownBy(() -> testSubject.payload(String.class.getName(), null, bytes("text")))
                    .isInstanceOf(DeadlineException.class);
        }
    }

    @Nested
    class MetadataValues {

        @Test
        void stringsNumbersAndBooleansBecomeStrings() {
            // given
            byte[] stored = bytes("{\"string\":\"text\",\"int\":3,\"decimal\":1.5,\"flag\":true}");

            // when
            Metadata metadata = testSubject.metadata(stored);

            // then
            assertThat(metadata).containsExactlyInAnyOrderEntriesOf(
                    Map.of("string", "text", "int", "3", "decimal", "1.5", "flag", "true")
            );
        }

        @Test
        void nestedMapsAndListsAreRenderedAsJson() {
            // given
            byte[] stored = bytes("{\"nested\":{\"key\":\"va\\\"lue\",\"list\":[1,\"two\",null,false]}}");

            // when
            Metadata metadata = testSubject.metadata(stored);

            // then
            assertThat(metadata).containsEntry("nested", "{\"key\":\"va\\\"lue\",\"list\":[1,\"two\",null,false]}");
        }

        @Test
        void noStoredMetadataIsReadAsEmptyMetadata() {
            // when / then
            assertThat(testSubject.metadata(null)).isEmpty();
        }

        @Test
        void storedMetadataIsReadBack() {
            // given
            Metadata original = Metadata.from(Map.of("key", "value", "other", "3"));

            // when
            Metadata metadata = testSubject.metadata(testSubject.toStored(original, String.class));

            // then
            assertThat(metadata).isEqualTo(original);
        }
    }

    @Nested
    class Scope {

        @Test
        void aStoredScopeIsConvertedBackIntoItsClass() {
            // given
            ScopeDescriptor original = new SagaScopeDescriptor("MySaga", "sagaId");

            // when
            ScopeDescriptor scope = testSubject.scope(SagaScopeDescriptor.class.getName(),
                                                      testSubject.toStored(original, byte[].class));

            // then
            assertThat(scope).isEqualTo(original);
        }

        @Test
        void anUnresolvableScopeClassFailsTheRead() {
            // when / then
            assertThatThrownBy(() -> testSubject.scope("com.example.RemovedScope", bytes("{}")))
                    .isInstanceOf(DeadlineException.class)
                    .hasMessageContaining("com.example.RemovedScope");
        }

        @Test
        void aScopeClassThatIsNoScopeDescriptorFailsTheRead() {
            // when / then
            assertThatThrownBy(() -> testSubject.scope(String.class.getName(), bytes("\"text\"")))
                    .isInstanceOf(DeadlineException.class);
        }

        @Test
        void anAggregateScopeIsStoredWithItsTypeAndIdentifier() {
            // given
            ScopeDescriptor original = new AggregateScopeDescriptor("MyAggregate", "aggregateId");

            // when
            String stored = testSubject.toStored(original, String.class);

            // then
            assertThat(stored).isEqualTo("{\"type\":\"MyAggregate\",\"identifier\":\"aggregateId\"}");
        }
    }

    /**
     * Axon Framework 4's {@code XStreamSerializer} stored a type's XStream alias instead of its class name, for XStream's
     * built-in aliases and for aliases the application registered.
     */
    @Nested
    @SuppressWarnings("removal")
    class XStreamAliases {

        @Test
        void aPayloadStoredUnderXStreamsUuidAliasIsReadAsAUuid() {
            // given
            UUID original = UUID.fromString("062833ae-eb22-38f6-9412-4f1837e7e900");
            StoredDeadlineConverter xStreamSubject = new StoredDeadlineConverter(new XStreamConverter(new XStream()));

            // when
            Object payload = xStreamSubject.payload("uuid", null, bytes("<uuid>" + original + "</uuid>"));

            // then
            assertThat(payload).isEqualTo(original);
        }

        @Test
        void aPayloadStoredUnderAnAliasOfTheApplicationIsReadThroughTheSameAlias() {
            // given
            XStream xStream = new XStream();
            xStream.alias("aliasedPayload", AliasedPayload.class);
            StoredDeadlineConverter xStreamSubject = new StoredDeadlineConverter(new XStreamConverter(xStream));
            AliasedPayload original = new AliasedPayload("label");
            byte[] stored = xStreamSubject.toStored(original, byte[].class);

            // when
            Object payload = xStreamSubject.payload("aliasedPayload", null, stored);

            // then
            assertThat(new String(stored, StandardCharsets.UTF_8)).startsWith("<aliasedPayload>");
            assertThat(payload).isEqualTo(original);
        }

        @Test
        void aScopeStoredUnderAnAliasIsReadThroughTheSameAlias() {
            // given
            XStream xStream = new XStream();
            xStream.alias("sagaScope", SagaScopeDescriptor.class);
            StoredDeadlineConverter xStreamSubject = new StoredDeadlineConverter(new XStreamConverter(xStream));
            ScopeDescriptor original = new SagaScopeDescriptor("MySaga", "sagaId");

            // when
            ScopeDescriptor scope = xStreamSubject.scope("sagaScope", xStreamSubject.toStored(original, byte[].class));

            // then
            assertThat(scope).isEqualTo(original);
        }

        @Test
        void anAliasIsResolvedThroughAnXStreamConverterWrappedAsTheEventConverter() {
            // given
            UUID original = UUID.fromString("062833ae-eb22-38f6-9412-4f1837e7e900");
            EventConverter eventConverter = new DelegatingEventConverter(
                    new DelegatingMessageConverter(new XStreamConverter(new XStream()))
            );
            StoredDeadlineConverter wrappedSubject = new StoredDeadlineConverter(eventConverter);

            // when
            Object payload = wrappedSubject.payload("uuid", null, bytes("<uuid>" + original + "</uuid>"));

            // then
            assertThat(payload).isEqualTo(original);
        }

        @Test
        void anAliasIsResolvedThroughAnXStreamConverterWrappedLikeTheDefaultEventConverter() {
            // given
            UUID original = UUID.fromString("062833ae-eb22-38f6-9412-4f1837e7e900");
            // the event converter chain Axon Framework builds around the general converter by default
            EventConverter eventConverter = new DelegatingEventConverter(new DelegatingMessageConverter(
                    new DelegatingGeneralConverter(new XStreamConverter(new XStream()))
            ));
            StoredDeadlineConverter wrappedSubject = new StoredDeadlineConverter(eventConverter);

            // when
            Object payload = wrappedSubject.payload("uuid", null, bytes("<uuid>" + original + "</uuid>"));

            // then
            assertThat(payload).isEqualTo(original);
        }

        @Test
        void theStringAndByteArrayAliasesResolveThroughTheXStreamMapper() {
            // given
            StoredDeadlineConverter xStreamSubject = new StoredDeadlineConverter(new XStreamConverter(new XStream()));

            // when
            Object stringPayload = xStreamSubject.payload("string", null, bytes("<string>text</string>"));
            Object bytesPayload = xStreamSubject.payload("byte-array", null, bytes("<byte-array>AQI=</byte-array>"));

            // then
            assertThat(stringPayload).isEqualTo("text");
            assertThat(bytesPayload).isEqualTo(new byte[]{1, 2});
        }

        @Test
        void anAliasIsNotResolvedWithAJsonConverter() {
            // when
            Object payload = testSubject.payload("uuid", null, bytes("\"062833ae-eb22-38f6-9412-4f1837e7e900\""));

            // then
            assertThat(payload).isInstanceOfSatisfying(
                    UnknownDeadlinePayload.class, unknown -> assertThat(unknown.typeName()).isEqualTo("uuid")
            );
        }
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Encloses a payload class, as a payload's stored type name has to keep its enclosing class.
     */
    static final class OuterPayload {

        record NestedPayload(String text, int number) {

        }
    }

    /**
     * A payload with a field, as a class XStream writes and reads by its fields.
     */
    static final class AliasedPayload {

        private final String label;

        AliasedPayload(String label) {
            this.label = label;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof AliasedPayload that && label.equals(that.label);
        }

        @Override
        public int hashCode() {
            return label.hashCode();
        }
    }
}
