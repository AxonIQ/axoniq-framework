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

package org.axonframework.conversion;

import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import org.axonframework.conversion.json.JacksonSerializer;

import java.beans.ConstructorProperties;
import java.util.Collection;
import java.util.EnumSet;

/**
 * Enumeration of serializers for testing purposes.
 *
 * @author JohT
 */
@SuppressWarnings("unused")
public enum TestSerializer {

    JACKSON {
        private final Serializer serializer = JacksonSerializer.defaultSerializer();

        @Override
        public Serializer getSerializer() {
            return serializer;
        }
    },
    CBOR {
        private final Serializer serializer = JacksonSerializer.builder()
                                                               .objectMapper(CBORMapper
                                                                                     .builder()
                                                                                     .findAndAddModules()
                                                                                     .build()).build();

        @Override
        public Serializer getSerializer() {
            return serializer;
        }
    },
    JACKSON_ONLY_ACCEPT_CONSTRUCTOR_PARAMETERS {
        private final Serializer serializer =
                JacksonSerializer.builder()
                                 .objectMapper(OnlyAcceptConstructorPropertiesAnnotation.attachTo(new ObjectMapper()))
                                 .build();

        @Override
        public Serializer getSerializer() {
            return serializer;
        }
    },
    JACKSON_IGNORE_NULL {
        private final ObjectMapper objectMapper = new ObjectMapper()
                .setSerializationInclusion(Include.NON_NULL);
        private final Serializer serializer = JacksonSerializer.builder()
                                                               .objectMapper(objectMapper)
                                                               .build();

        @Override
        public Serializer getSerializer() {
            return serializer;
        }
    };

    protected byte[] serialize(Object object) {
        return getSerializer().serialize(object, byte[].class).getData();
    }

    protected <T> T deserialize(byte[] serialized, Class<T> type) {
        return getSerializer().deserialize(asSerializedData(serialized, type));
    }

    public abstract Serializer getSerializer();

    @SuppressWarnings("unchecked")
    public <T> T serializeDeserialize(T object) {
        return deserialize(serialize(object), (Class<T>) object.getClass());
    }

    public static Collection<TestConverter> all() {
        return EnumSet.allOf(TestConverter.class);
    }

    static <T> SerializedObject<byte[]> asSerializedData(byte[] serialized, Class<T> type) {
        SimpleSerializedType serializedType = new SimpleSerializedType(type.getName(), null);
        return new SimpleSerializedObject<>(serialized, byte[].class, serializedType);
    }

    private static class OnlyAcceptConstructorPropertiesAnnotation extends JacksonAnnotationIntrospector {

        public static ObjectMapper attachTo(ObjectMapper objectMapper) {
            return objectMapper.setAnnotationIntrospector(new OnlyAcceptConstructorPropertiesAnnotation());
        }

        @Override
        public Mode findCreatorAnnotation(MapperConfig<?> config, Annotated annotated) {
            return (annotated.hasAnnotation(ConstructorProperties.class))
                    ? super.findCreatorAnnotation(config, annotated)
                    : Mode.DISABLED;
        }
    }
}