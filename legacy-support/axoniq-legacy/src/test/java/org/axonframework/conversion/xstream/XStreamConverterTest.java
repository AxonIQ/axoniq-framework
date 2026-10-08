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

package org.axonframework.conversion.xstream;

import com.thoughtworks.xstream.XStream;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.modelling.saga.repository.StubSaga;
import org.junit.jupiter.api.*;

import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class XStreamConverterTest {

    private XStream xStream;
    private Converter testSubject;

    @BeforeEach
    void setUp() {
        xStream = new XStream();
        testSubject = new XStreamConverter(xStream);
    }

    @Test
    void constructionRequiresNonNullXStream() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> new XStreamConverter(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void convertingNullInputReturnsNull() {
        // given / when
        Object result = testSubject.convert(null, StubSaga.class);

        // then
        assertThat(result).isNull();
    }

    @Test
    void convertingToTheSourceTypeReturnsTheInputUnchanged() {
        // given
        StubSaga saga = new StubSaga();
        saga.handled("OrderPlaced");

        // when
        StubSaga result = testSubject.convert(saga, StubSaga.class);

        // then
        assertThat(result).isSameAs(saga);
    }

    @Nested
    class XmlRoundTrips {

        @Test
        void aPojoRoundTripsThroughByteArray() {
            // given
            StubSaga saga = new StubSaga();
            saga.handled("OrderPlaced");
            saga.handled("OrderPaid");

            // when
            byte[] converted = testSubject.convert(saga, byte[].class);
            StubSaga result = testSubject.convert(converted, StubSaga.class);

            // then
            assertThat(result).isEqualTo(saga);
        }

        @Test
        void aPojoRoundTripsThroughString() {
            // given
            StubSaga saga = new StubSaga();
            saga.handled("OrderShipped");

            // when
            String converted = testSubject.convert(saga, String.class);
            StubSaga result = testSubject.convert(converted, StubSaga.class);

            // then
            assertThat(converted).contains("OrderShipped");
            assertThat(result).isEqualTo(saga);
        }

        @Test
        void aPojoRoundTripsThroughInputStream() {
            // given
            StubSaga saga = new StubSaga();
            saga.handled("OrderShipped");

            // when
            InputStream converted = testSubject.convert(saga, InputStream.class);
            StubSaga result = testSubject.convert(converted, StubSaga.class);

            // then
            assertThat(result).isEqualTo(saga);
        }

        @Test
        void convertingToAParameterizedTypeResolvesItsRawClass() {
            // given a TypeReference, the way StoredDeadlineConverter resolves metadata read back from storage
            Map<String, Object> map = new HashMap<>();
            map.put("traceId", "abc-123");
            String xml = testSubject.convert(map, String.class);
            Type mapType = new TypeReference<Map<String, Object>>() {
            }.getType();

            // when
            Map<String, Object> result = testSubject.convert(xml, mapType);

            // then
            assertThat(result).containsExactlyEntriesOf(map);
        }
    }

    @Nested
    class ContentTypePassThrough {

        @Test
        void byteArrayToStringDoesNotInvokeXStream() {
            // given some content that is not valid XStream XML
            byte[] plainText = "not-xml-at-all".getBytes(StandardCharsets.UTF_8);

            // when
            String result = testSubject.convert(plainText, String.class);

            // then
            assertThat(result).isEqualTo("not-xml-at-all");
        }

        @Test
        void stringToByteArrayDoesNotInvokeXStream() {
            // given / when
            byte[] result = testSubject.convert("not-xml-at-all", byte[].class);

            // then
            assertThat(result).isEqualTo("not-xml-at-all".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Nested
    class MetadataConversion {

        @Test
        void nonEmptyMetadataRoundTripsAndProducesAMetaDataElement() {
            // given
            Metadata metadata = Metadata.with("traceId", "abc-123").and("userId", "steven");

            // when
            String xml = testSubject.convert(metadata, String.class);
            Metadata result = testSubject.convert(xml, Metadata.class);

            // then
            assertThat(xml).contains("<meta-data>");
            assertThat(result).containsExactlyInAnyOrderEntriesOf(metadata);
        }

        @Test
        void metadataWithANonStringValueIsCoercedToStringRatherThanThrowing() {
            // given XML the way an Axon Framework 4 XStreamSerializer writes a non-String metadata value
            String xml = """
                    <meta-data>
                      <entry>
                        <string>retries</string>
                        <int>3</int>
                      </entry>
                    </meta-data>""";

            // when
            Metadata result = testSubject.convert(xml, Metadata.class);

            // then
            assertThat(result).containsEntry("retries", "3");
        }

        @Test
        void emptyMetadataRoundTrips() {
            // given
            Metadata metadata = Metadata.emptyInstance();

            // when
            String xml = testSubject.convert(metadata, String.class);
            Metadata result = testSubject.convert(xml, Metadata.class);

            // then
            assertThat(result).isEmpty();
        }
    }

    @Nested
    class ErrorHandling {

        @Test
        void malformedXmlResultsInAConversionException() {
            // given
            String malformedXml = "<not-closed>";

            // when / then
            assertThatThrownBy(() -> testSubject.convert(malformedXml, StubSaga.class))
                    .isInstanceOf(ConversionException.class);
        }

        @Test
        void convertingBetweenTwoNonCarrierTypesResultsInAConversionException() {
            // given
            StubSaga saga = new StubSaga();

            // when / then
            assertThatThrownBy(() -> testSubject.convert(saga, Metadata.class))
                    .isInstanceOf(ConversionException.class);
        }

        @Test
        void readingXmlOfTheWrongTypeResultsInAConversionExceptionRatherThanAClassCastException() {
            // given XML holding a Metadata, not a StubSaga
            String xml = testSubject.convert(Metadata.with("traceId", "abc-123"), String.class);

            // when / then
            assertThatThrownBy(() -> testSubject.convert(xml, StubSaga.class))
                    .isInstanceOf(ConversionException.class)
                    .hasMessageContaining(Metadata.class.getName())
                    .hasMessageContaining(StubSaga.class.getName());
        }
    }
}
