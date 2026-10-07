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

package org.axonframework.deadline;

import com.thoughtworks.xstream.XStream;
import org.axonframework.conversion.ChainingContentTypeConverter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.conversion.xstream.XStreamConverter;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.modelling.command.AggregateScopeDescriptor;
import org.axonframework.modelling.saga.SagaScopeDescriptor;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;

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
}
