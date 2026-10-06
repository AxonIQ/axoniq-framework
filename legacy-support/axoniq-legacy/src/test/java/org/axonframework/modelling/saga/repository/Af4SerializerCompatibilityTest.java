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
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies compatibility between the serialized representation produced by Axon Framework 4 and the converter used by
 * the legacy saga stores.
 *
 * @author Mateusz Nowak
 */
class Af4SerializerCompatibilityTest {

    private static final Path AF4_MESSAGING_JAR = Path.of(
            "target", "af4-serializer", "axon-messaging-af4.jar"
    );

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

        private final Converter xStreamConverter = new XStreamConverter(allowingXStream());

        @Test
        void xStreamSerializerSagaIsConvertableByXStreamConverter() throws Exception {
            // given
            StubSaga saga = new StubSaga();
            saga.handled("OrderPlaced");
            saga.handled("OrderPaid");

            // when
            String af4Xml = Af4XStreamSupport.withAf4ClassLoader(
                    classLoader -> Af4XStreamSupport.af4XStream(classLoader).toXML(saga)
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
            StubSaga deserialized = Af4XStreamSupport.withAf4ClassLoader(
                    classLoader -> (StubSaga) Af4XStreamSupport.af4XStream(classLoader).fromXML(af5Xml)
            );

            // then
            assertThat(deserialized).isEqualTo(saga);
        }

        @Test
        void xStreamSerializedMetadataIsConvertableByXStreamConverter() throws Exception {
            // given
            Map<String, String> entries = Map.of("traceId", "abc-123", "userId", "steven");

            // when
            String af4Xml = Af4XStreamSupport.withAf4ClassLoader(classLoader -> {
                Object af4MetaData = classLoader.loadClass("org.axonframework.messaging.MetaData")
                                                .getConstructor(Map.class)
                                                .newInstance(entries);
                return Af4XStreamSupport.af4XStream(classLoader).toXML(af4MetaData);
            });
            Metadata converted = xStreamConverter.convert(af4Xml, Metadata.class);

            // then
            assertThat(converted).containsExactlyInAnyOrderEntriesOf(entries);
        }

        @Test
        void xStreamConvertedMetadataIsDeserializedByXStreamSerializer() throws Exception {
            // given
            Metadata metadata = Metadata.with("traceId", "abc-123").and("userId", "steven");

            // when
            String af5Xml = xStreamConverter.convert(metadata, String.class);
            @SuppressWarnings("unchecked")
            Map<String, Object> deserialized = Af4XStreamSupport.withAf4ClassLoader(
                    classLoader -> (Map<String, Object>) Af4XStreamSupport.af4XStream(classLoader).fromXML(af5Xml)
            );

            // then
            assertThat(deserialized).containsExactlyInAnyOrderEntriesOf(metadata);
        }

        @Test
        void aggregateScopeDescriptorRoundTripsBothWays() throws Exception {
            // given
            AggregateScopeDescriptor descriptor = new AggregateScopeDescriptor("aggregateType", "aggregateId");

            // when
            String af4Xml = Af4XStreamSupport.withAf4ClassLoader(
                    classLoader -> Af4XStreamSupport.af4XStream(classLoader)
                                                    .toXML(Af4XStreamSupport.construct(
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
            Af4XStreamSupport.withAf4ClassLoader(classLoader -> {
                Object fromAf5 = Af4XStreamSupport.af4XStream(classLoader).fromXML(af5Xml);
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
            String af4Xml = Af4XStreamSupport.withAf4ClassLoader(
                    classLoader -> Af4XStreamSupport.af4XStream(classLoader)
                                                    .toXML(Af4XStreamSupport.construct(
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
            Af4XStreamSupport.withAf4ClassLoader(classLoader -> {
                Object fromAf5 = Af4XStreamSupport.af4XStream(classLoader).fromXML(af5Xml);
                assertThat(fromAf5.getClass().getMethod("getType").invoke(fromAf5)).isEqualTo(descriptor.getType());
                assertThat(fromAf5.getClass().getMethod("getIdentifier").invoke(fromAf5))
                        .isEqualTo(descriptor.getIdentifier());
                return null;
            });
        }

        private XStream allowingXStream() {
            XStream xStream = new XStream();
            xStream.allowTypesByWildcard(new String[]{"org.axonframework.**"});
            return xStream;
        }
    }

    private static Af4SerializedObject serializeWithAf4JacksonSerializer(Object value) throws Exception {
        assertThat(AF4_MESSAGING_JAR).isRegularFile();
        URL serializerJar = AF4_MESSAGING_JAR.toUri().toURL();
        try (URLClassLoader classLoader = new AxonFramework4ClassLoader(serializerJar)) {
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
        }
    }

    private record Af4SerializedObject(byte[] data, String typeName) {

    }

    private static final class AxonFramework4ClassLoader extends URLClassLoader {

        private AxonFramework4ClassLoader(URL serializerJar) {
            super(new URL[]{serializerJar}, Af4SerializerCompatibilityTest.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> loadedClass = findLoadedClass(name);
                if (loadedClass == null && name.startsWith("org.axonframework.")) {
                    try {
                        loadedClass = findClass(name);
                    } catch (ClassNotFoundException ignored) {
                        // The saga test fixture is an AF5 class and therefore comes from the parent class loader.
                    }
                }
                if (loadedClass == null) {
                    loadedClass = super.loadClass(name, false);
                }
                if (resolve) {
                    resolveClass(loadedClass);
                }
                return loadedClass;
            }
        }
    }
}
