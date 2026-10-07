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

package org.axonframework.modelling.saga.repository;

import com.thoughtworks.xstream.XStream;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.conversion.xstream.XStreamConverter;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.modelling.command.AggregateScopeDescriptor;
import org.axonframework.modelling.saga.SagaScopeDescriptor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies compatibility between the serialized representation produced by Axon Framework 4 and the converter used by
 * the legacy saga stores.
 *
 * @author Mateusz Nowak
 */
class Af4SerializerCompatibilityTest {

    @Nested
    class JacksonSerialization {

        @Test
        void sagaSerializedByAf4SerializerIsConvertedByAf5Converter() throws Exception {
            // given
            StubSaga saga = new StubSaga();
            saga.handled("OrderPlaced");
            saga.handled("OrderPaid");

            // when
            Af4SerializedObject serialized = serializeWithAf4JacksonSerializer(saga);
            StubSaga converted = new JacksonConverter().convert(serialized.data(), StubSaga.class);

            // then
            assertThat(serialized.typeName()).isEqualTo(StubSaga.class.getName());
            assertThat(converted).isEqualTo(saga);
        }
    }

    @Nested
    class XStreamSerialization {

        private final Converter xStreamConverter = new XStreamConverter(new XStream());

        @Test
        void xStreamSerializerSagaIsConvertableByXStreamConverter() throws Exception {
            // given
            StubSaga saga = new StubSaga();
            saga.handled("OrderPlaced");
            saga.handled("OrderPaid");

            // when
            String af4Xml = Af4ClassLoaderSupport.withAf4ClassLoader(
                    classLoader -> Af4ClassLoaderSupport.af4XStream(classLoader).toXML(saga)
            );
            StubSaga converted = xStreamConverter.convert(af4Xml, StubSaga.class);

            // then
            assertThat(converted).isEqualTo(saga);
        }

        @Test
        void xStreamConvertedSagaIsDeserializableByXStreamSerializer() throws Exception {
            // given
            StubSaga saga = new StubSaga();
            saga.handled("OrderShipped");

            // when
            String af5Xml = xStreamConverter.convert(saga, String.class);
            StubSaga deserialized = Af4ClassLoaderSupport.withAf4ClassLoader(
                    classLoader -> (StubSaga) Af4ClassLoaderSupport.af4XStream(classLoader).fromXML(af5Xml)
            );

            // then
            assertThat(deserialized).isEqualTo(saga);
        }

        @Test
        void xStreamSerializedMetadataIsConvertableByXStreamConverter() throws Exception {
            // given
            Map<String, Object> entries = Map.of("traceId", "abc-123", "userId", "steven");

            // when
            String af4Xml = Af4ClassLoaderSupport.withAf4ClassLoader(classLoader -> {
                Object af4MetaData = classLoader.loadClass("org.axonframework.messaging.MetaData")
                                                .getConstructor(Map.class)
                                                .newInstance(entries);
                return Af4ClassLoaderSupport.af4XStream(classLoader).toXML(af4MetaData);
            });
            Metadata converted = xStreamConverter.convert(af4Xml, Metadata.class);

            // then
            assertThat(converted).containsExactlyInAnyOrderEntriesOf(Map.of("traceId", "abc-123", "userId", "steven"));
        }

        @Test
        void xStreamConvertedMetadataIsDeserializedByXStreamSerializer() throws Exception {
            // given
            Metadata metadata = Metadata.with("traceId", "abc-123").and("userId", "steven");

            // when
            String af5Xml = xStreamConverter.convert(metadata, String.class);
            @SuppressWarnings("unchecked")
            Map<String, Object> deserialized = Af4ClassLoaderSupport.withAf4ClassLoader(
                    classLoader -> (Map<String, Object>) Af4ClassLoaderSupport.af4XStream(classLoader).fromXML(af5Xml)
            );

            // then
            assertThat(deserialized).containsExactlyInAnyOrderEntriesOf(metadata);
        }

        @Test
        void xStreamSerializedEmptyMetadataIsConvertableByXStreamConverter() throws Exception {
            // given
            String af4Xml = Af4ClassLoaderSupport.withAf4ClassLoader(classLoader -> {
                Object af4MetaData = classLoader.loadClass("org.axonframework.messaging.MetaData")
                                                .getConstructor(Map.class)
                                                .newInstance(Map.of());
                return Af4ClassLoaderSupport.af4XStream(classLoader).toXML(af4MetaData);
            });

            // when
            Metadata converted = xStreamConverter.convert(af4Xml, Metadata.class);

            // then
            assertThat(converted).isEmpty();
        }

        @Test
        void xStreamSerializedUuidIsConvertableByXStreamConverter() throws Exception {
            // given
            UUID id = UUID.randomUUID();
            String af4Xml = Af4ClassLoaderSupport.withAf4ClassLoader(
                    classLoader -> Af4ClassLoaderSupport.af4XStream(classLoader).toXML(id)
            );

            // when
            UUID converted = xStreamConverter.convert(af4Xml, UUID.class);

            // then
            assertThat(af4Xml).contains("<uuid>");
            assertThat(converted).isEqualTo(id);
        }

        @Test
        void xStreamConvertedUuidIsDeserializedByXStreamSerializer() throws Exception {
            // given
            UUID id = UUID.randomUUID();

            // when
            String af5Xml = xStreamConverter.convert(id, String.class);
            UUID deserialized = Af4ClassLoaderSupport.withAf4ClassLoader(
                    classLoader -> (UUID) Af4ClassLoaderSupport.af4XStream(classLoader).fromXML(af5Xml)
            );

            // then
            assertThat(deserialized).isEqualTo(id);
        }

        @Test
        void xStreamSerializedNonAsciiPayloadIsConvertableThroughByteArray() throws Exception {
            // given
            StubSaga saga = new StubSaga();
            saga.handled("Café 日本語");
            byte[] af4Bytes = Af4ClassLoaderSupport.withAf4ClassLoader(
                    classLoader -> Af4ClassLoaderSupport.af4XStream(classLoader).toXML(saga)
            ).getBytes(StandardCharsets.UTF_8);

            // when
            StubSaga converted = xStreamConverter.convert(af4Bytes, StubSaga.class);

            // then
            assertThat(converted).isEqualTo(saga);
        }

        @Test
        void xStreamConvertedNonAsciiPayloadIsDeserializableThroughByteArray() throws Exception {
            // given
            StubSaga saga = new StubSaga();
            saga.handled("Café 日本語");

            // when
            byte[] af5Bytes = xStreamConverter.convert(saga, byte[].class);
            StubSaga deserialized = Af4ClassLoaderSupport.withAf4ClassLoader(
                    classLoader -> (StubSaga) Af4ClassLoaderSupport.af4XStream(classLoader)
                                                                    .fromXML(new String(af5Bytes,
                                                                                         StandardCharsets.UTF_8))
            );

            // then
            assertThat(deserialized).isEqualTo(saga);
        }

        @Test
        void aggregateScopeDescriptorRoundTripsBothWays() throws Exception {
            // given
            AggregateScopeDescriptor descriptor = new AggregateScopeDescriptor("aggregateType", "aggregateId");

            // when
            String af4Xml = Af4ClassLoaderSupport.withAf4ClassLoader(
                    classLoader -> Af4ClassLoaderSupport.af4XStream(classLoader)
                                                    .toXML(Af4ClassLoaderSupport.construct(
                                                            classLoader,
                                                            "org.axonframework.modelling.command.AggregateScopeDescriptor",
                                                            descriptor.getType(),
                                                            descriptor.getIdentifier()
                                                    ))
            );
            AggregateScopeDescriptor fromAf4 = xStreamConverter.convert(af4Xml, AggregateScopeDescriptor.class);

            // then
            assertThat(fromAf4.getType()).isEqualTo(descriptor.getType());
            assertThat(fromAf4.getIdentifier()).isEqualTo(descriptor.getIdentifier());

            // when: Axon Framework 4 reads what this Converter wrote. The AF4 classloader resolves this XML's
            // element name to its own AggregateScopeDescriptor class (same name, loaded from the AF4 jar), as a real
            // Axon Framework 4 node would, so its fields are asserted reflectively rather than casting to the Axon
            // Framework 5 class.
            String af5Xml = xStreamConverter.convert(descriptor, String.class);
            Af4ClassLoaderSupport.withAf4ClassLoader(classLoader -> {
                Object fromAf5 = Af4ClassLoaderSupport.af4XStream(classLoader).fromXML(af5Xml);
                assertThat(fromAf5.getClass().getMethod("getType").invoke(fromAf5)).isEqualTo(descriptor.getType());
                assertThat(fromAf5.getClass().getMethod("getIdentifier").invoke(fromAf5))
                        .isEqualTo(descriptor.getIdentifier());
                return null;
            });
        }

        @Test
        void sagaScopeDescriptorRoundTripsBothWays() throws Exception {
            // given
            SagaScopeDescriptor descriptor = new SagaScopeDescriptor("sagaType", "sagaId");

            // when
            String af4Xml = Af4ClassLoaderSupport.withAf4ClassLoader(
                    classLoader -> Af4ClassLoaderSupport.af4XStream(classLoader)
                                                    .toXML(Af4ClassLoaderSupport.construct(
                                                            classLoader,
                                                            "org.axonframework.modelling.saga.SagaScopeDescriptor",
                                                            descriptor.getType(),
                                                            descriptor.getIdentifier()
                                                    )));
            SagaScopeDescriptor fromAf4 = xStreamConverter.convert(af4Xml, SagaScopeDescriptor.class);

            // then
            assertThat(fromAf4).isEqualTo(descriptor);

            // when: Axon Framework 4 reads what this Converter wrote; see the equivalent note in
            // aggregateScopeDescriptorRoundTripsBothWays() on why this is asserted reflectively.
            String af5Xml = xStreamConverter.convert(descriptor, String.class);
            Af4ClassLoaderSupport.withAf4ClassLoader(classLoader -> {
                Object fromAf5 = Af4ClassLoaderSupport.af4XStream(classLoader).fromXML(af5Xml);
                assertThat(fromAf5.getClass().getMethod("getType").invoke(fromAf5)).isEqualTo(descriptor.getType());
                assertThat(fromAf5.getClass().getMethod("getIdentifier").invoke(fromAf5))
                        .isEqualTo(descriptor.getIdentifier());
                return null;
            });
        }
    }

    private static Af4SerializedObject serializeWithAf4JacksonSerializer(Object value) throws Exception {
        return Af4ClassLoaderSupport.withAf4ClassLoader(classLoader -> {
            Class<?> serializerContract = classLoader.loadClass("org.axonframework.serialization.Serializer");
            Class<?> serializerType = classLoader.loadClass(
                    "org.axonframework.serialization.json.JacksonSerializer"
            );
            Object serializer = serializerType.getMethod("defaultSerializer").invoke(null);
            assertThat(serializer).isInstanceOf(serializerContract);

            Object serialized = serializerType.getMethod("serialize", Object.class, Class.class)
                                              .invoke(serializer, value, byte[].class);
            Class<?> serializedObjectType = classLoader.loadClass(
                    "org.axonframework.serialization.SerializedObject"
            );
            byte[] data = (byte[]) serializedObjectType.getMethod("getData").invoke(serialized);
            Object type = serializedObjectType.getMethod("getType").invoke(serialized);
            Method getName = type.getClass().getMethod("getName");
            return new Af4SerializedObject(data, (String) getName.invoke(type));
        });
    }

    private record Af4SerializedObject(byte[] data, String typeName) {

    }
}
