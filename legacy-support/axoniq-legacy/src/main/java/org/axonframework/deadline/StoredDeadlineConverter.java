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

import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.Metadata;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.core.json.JsonFactory;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Converts the parts of a deadline that the persistent deadline managers store, its payload, metadata and scope
 * descriptor, to and from their stored form, keeping the layout Axon Framework 4.13 wrote.
 * <p>
 * The stored form holds the payload's type name next to the payload, and the class name next to the scope descriptor.
 * The type name {@value #EMPTY_TYPE} marks a deadline without a payload, as in Axon Framework 4. Any other type name is
 * resolved to a class, and the stored data is converted into it with the configured {@link Converter}. A type name that
 * resolves to no class yields an {@link UnknownDeadlinePayload} instead of a failure, as Axon Framework 4's
 * {@code UnknownSerializedType} did.
 * <p>
 * A {@code String} or {@code byte[]} payload is stored the way Axon Framework 4's serializers stored it: as a JSON
 * string, Base64-encoded for a {@code byte[]}, or as an XStream {@code <string>} or {@code <byte-array>} element. A
 * {@link Converter} treats a {@code String} or {@code byte[]} as content that is already in its stored form, so it
 * would store such a payload as is. Which of the two forms to write follows from the configured converter's format,
 * which is detected once, by converting a probe value. The JSON string is written and read with Jackson's streaming
 * JSON writer and parser, and the XStream element with XStream itself, so that both match what Axon Framework 4
 * wrote. A converter
 * that writes neither JSON nor XStream XML cannot store such a payload, and scheduling one fails. Reading follows the
 * stored form, and also accepts the type names {@code string} and {@code byte-array}, under which Axon Framework 4's
 * {@code XStreamSerializer} stored these payloads.
 * <p>
 * Metadata is read untyped, into a {@code Map<String, Object>}, and each value is turned into a {@code String}: Axon
 * Framework 4 metadata could hold any value, while Axon Framework 5 metadata holds strings only. Strings stay as they
 * are, numbers and booleans become their {@link String#valueOf(Object) string value}, and maps and lists are rendered
 * as JSON by Jackson's streaming JSON writer, so the form does not depend on the converter's storage format.
 * <p>
 * This class is internal, as it only serves the deadline managers of this module, which share the stored layout.
 *
 * @author Jakob Hatzl
 * @since 5.4.0
 */
@Internal
public final class StoredDeadlineConverter {

    /**
     * The stored type name of a deadline without a payload. Axon Framework 4 nodes read it back as a {@code null}
     * payload without loading a class.
     */
    public static final String EMPTY_TYPE = "empty";

    private static final String SERIALIZED_NULL = "null";

    /**
     * The concrete type metadata is read into. A converter with default typing, such as a mirror of Axon Framework 4's
     * {@code Jackson3Serializer} with {@code defaultTyping()}, expects type information for an abstract target type,
     * which Axon Framework 4 never wrote around the metadata, as it read its concrete {@code MetaData} class.
     */
    private static final TypeReference<LinkedHashMap<String, Object>> JACKSON_UNTYPED_MAP = new TypeReference<>() {
    };
    /**
     * The type XStream XML metadata is read into. XStream restores Axon Framework 4's {@code <meta-data>} element as
     * {@link Metadata}, which is a {@code Map} but no {@code LinkedHashMap}.
     */
    private static final TypeReference<Map<String, Object>> XSTREAM_UNTYPED_MAP = new TypeReference<>() {
    };
    private static final MessageTypeResolver MESSAGE_TYPE_RESOLVER = new ClassBasedMessageTypeResolver();
    /**
     * Writes and reads the JSON strings of {@code String} and {@code byte[]} payloads, and renders maps and lists in
     * metadata as JSON, independent of the configured converter, as neither form depends on a mapper's configuration.
     * Only Jackson's streaming core is used, which has no dependencies of its own, so it cannot clash with the Jackson
     * version an application's converter uses.
     */
    private static final JsonFactory JSON_FACTORY = JsonFactory.builder().build();
    private static final String INVALID_JSON_STRING = "The stored payload is no valid JSON string";
    private static final String XSTREAM_CLASS_NAME = "com.thoughtworks.xstream.XStream";
    /**
     * The type names Axon Framework 4's {@code XStreamSerializer} stored a {@code String} and a {@code byte[]} payload
     * under: XStream's built-in aliases of these types, instead of their class names.
     */
    private static final Map<String, Class<?>> XSTREAM_TYPE_NAMES =
            Map.of("string", String.class, "byte-array", byte[].class);

    private final Converter converter;
    private final StoredFormat storedFormat;

    /**
     * Creates a {@code StoredDeadlineConverter} converting with the given {@code converter}.
     *
     * @param converter the converter matching the stored form of the deadlines
     */
    public StoredDeadlineConverter(Converter converter) {
        this.converter = Objects.requireNonNull(converter, "The Converter may not be null.");
        this.storedFormat = StoredFormat.of(converter);
    }

    /**
     * Returns the type name to store for the given {@code payload}: its class name, or {@value #EMPTY_TYPE} if there is
     * no payload.
     *
     * @param payload the payload of the deadline to store
     * @return the type name to store with the payload
     */
    public static String typeNameOf(@Nullable Object payload) {
        return payload == null ? EMPTY_TYPE : payload.getClass().getName();
    }

    /**
     * Returns the {@link MessageType} of a deadline carrying the given {@code payload}, derived from the payload's
     * class, as the stored form keeps no message type.
     *
     * @param payload the payload of the deadline
     * @return the message type derived from the payload's class
     */
    public static MessageType messageTypeOf(@Nullable Object payload) {
        return MESSAGE_TYPE_RESOLVER.resolveOrThrow(payload == null ? Void.class : payload.getClass());
    }

    /**
     * Converts the given deadline {@code payload} into the given stored {@code representation}, {@code byte[]} or
     * {@code String}. Without a payload, this is the serialized {@code null}, the JSON {@code null}, as Axon Framework
     * 4's {@code JacksonSerializer} wrote it: Axon Framework 4 nodes do not read the data stored under the type name
     * {@value #EMPTY_TYPE}, but reject a job whose data is missing. A {@code String} or {@code byte[]} payload is
     * stored as Axon Framework 4 stored it in the configured converter's format.
     *
     * @param payload        the payload of the deadline to store, if any
     * @param representation the stored representation, {@code byte[]} or {@code String}
     * @param <T>            the stored representation
     * @return the stored form of the given {@code payload}
     * @throws DeadlineException if the payload is a {@code String} or {@code byte[]}, and the configured converter
     *                           writes neither JSON nor XStream XML
     */
    public <T> @Nullable T payloadToStored(@Nullable Object payload, Class<T> representation) {
        if (payload == null) {
            return asRepresentation(SERIALIZED_NULL, representation);
        }
        if (payload instanceof String || payload instanceof byte[]) {
            return asRepresentation(storedFormat.write(payload), representation);
        }
        return converter.convert(payload, representation);
    }

    /**
     * Converts the given {@code value} into the given stored {@code representation}, such as {@code byte[]} or
     * {@code String}.
     *
     * @param value          the payload, metadata or scope descriptor to store
     * @param representation the stored representation
     * @param <T>            the stored representation
     * @return the stored form of the given {@code value}, or {@code null} if it is {@code null}
     */
    public <T> @Nullable T toStored(@Nullable Object value, Class<T> representation) {
        return converter.convert(value, representation);
    }

    /**
     * Converts the given stored {@code data} into the given {@code type}, for stored structures that are converted as a
     * whole, such as JobRunr's {@link org.axonframework.deadline.jobrunr.DeadlineDetails}.
     *
     * @param data the stored data
     * @param type the type to convert the stored data into
     * @param <T>  the type to convert the stored data into
     * @return the converted data
     * @throws DeadlineException if the data converts to {@code null}
     */
    public <T> T fromStored(Object data, Class<T> type) {
        T converted = converter.convert(data, type);
        if (converted == null) {
            throw new DeadlineException("The stored " + type.getSimpleName() + " converted to null");
        }
        return converted;
    }

    /**
     * Converts the given stored payload into the class named by the given {@code typeName}.
     *
     * @param typeName the stored type name of the payload
     * @param revision the stored revision of the payload type, read but not used for the conversion
     * @param data     the stored payload
     * @return {@code null} for the type name {@value #EMPTY_TYPE}, an {@link UnknownDeadlinePayload} if the type name
     * resolves to no class, and the converted payload otherwise
     * @throws DeadlineException if a {@code String} or {@code byte[]} payload is stored in neither form Axon
     *                           Framework 4 stored it in
     */
    public @Nullable Object payload(String typeName, @Nullable String revision, @Nullable Object data) {
        if (EMPTY_TYPE.equals(typeName)) {
            return null;
        }
        Optional<Class<?>> payloadType = Optional.<Class<?>>ofNullable(XSTREAM_TYPE_NAMES.get(typeName))
                                                 .or(() -> resolve(typeName));
        if (payloadType.isEmpty()) {
            return new UnknownDeadlinePayload(typeName, revision, data);
        }
        Class<?> type = payloadType.get();
        if (type == String.class || type == byte[].class) {
            return storedValue(type, data);
        }
        return converter.convert(data, type);
    }

    /**
     * Converts the given stored scope descriptor into the class named by the given {@code className}.
     *
     * @param className the stored class name of the scope descriptor
     * @param data      the stored scope descriptor
     * @return the converted scope descriptor
     * @throws DeadlineException if the class name resolves to no {@link ScopeDescriptor} class, or the data converts to
     *                           no scope descriptor
     */
    public ScopeDescriptor scope(String className, Object data) {
        Class<?> scopeType = resolve(className).orElseThrow(() -> new DeadlineException(
                "The scope descriptor of the deadline is of the unknown class [" + className + "]"
        ));
        if (!ScopeDescriptor.class.isAssignableFrom(scopeType)) {
            throw new DeadlineException("The class [" + className + "] of the deadline's scope is no ScopeDescriptor");
        }
        Object scope = converter.convert(data, scopeType);
        if (scope == null) {
            throw new DeadlineException("The scope descriptor of the deadline converted to null");
        }
        return (ScopeDescriptor) scope;
    }

    /**
     * Converts the given stored metadata into {@link Metadata}. Each value becomes a {@code String}: strings stay as
     * they are, numbers and booleans become their string value, and maps and lists are rendered as JSON.
     *
     * @param data the stored metadata, or {@code null} if none was stored
     * @return the converted metadata, empty if none was stored
     */
    public Metadata metadata(@Nullable Object data) {
        Map<String, Object> untyped = converter.convert(
                data, storedFormat == StoredFormat.XSTREAM ? XSTREAM_UNTYPED_MAP.getType() : JACKSON_UNTYPED_MAP.getType()
        );
        if (untyped == null) {
            return Metadata.emptyInstance();
        }
        Map<String, String> metadata = new LinkedHashMap<>();
        untyped.forEach((key, value) -> metadata.put(key, asString(value)));
        return Metadata.from(metadata);
    }

    /**
     * Reads a stored {@code String} or {@code byte[]} payload: a JSON string or an XStream element, both directly, as a
     * converter would hand either back in its stored form.
     */
    private Object storedValue(Class<?> type, @Nullable Object data) {
        String stored = data instanceof byte[] bytes
                ? new String(bytes, StandardCharsets.UTF_8)
                : data instanceof String string ? string : converter.convert(data, String.class);
        String trimmed = stored == null ? "" : stored.strip();
        Object value;
        if (trimmed.startsWith("\"")) {
            value = readJson(type, trimmed);
        } else if (trimmed.startsWith("<")) {
            requireXStream();
            value = XStreamPayloadFormat.read(trimmed);
        } else {
            value = null;
        }
        if (!type.isInstance(value)) {
            throw new DeadlineException(
                    "The stored payload of type [" + type.getTypeName() + "] is neither a JSON string nor an XStream "
                            + "element, as Axon Framework 4 stored it"
            );
        }
        return value;
    }

    private static <T> T asRepresentation(String stored, Class<T> representation) {
        if (representation == byte[].class) {
            return representation.cast(stored.getBytes(StandardCharsets.UTF_8));
        }
        return representation.cast(stored);
    }

    /**
     * The single point resolving a stored type or class name to a class, so that resolving names a converter maps
     * differently stays in one place.
     */
    private static Optional<Class<?>> resolve(String typeName) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader classLoader = contextClassLoader != null
                ? contextClassLoader
                : StoredDeadlineConverter.class.getClassLoader();
        try {
            return Optional.of(Class.forName(typeName, false, classLoader));
        } catch (ClassNotFoundException | LinkageError e) {
            return Optional.empty();
        }
    }

    private static @Nullable String asString(@Nullable Object value) {
        if (value == null || value instanceof String) {
            return (String) value;
        }
        if (value instanceof Map<?, ?> || value instanceof List<?>) {
            return writeJson(value);
        }
        return String.valueOf(value);
    }

    private static String writeJson(Object value) {
        StringWriter json = new StringWriter();
        try (JsonGenerator generator = JSON_FACTORY.createGenerator(ObjectWriteContext.empty(), json)) {
            writeJson(value, generator);
        }
        return json.toString();
    }

    private static void writeJson(@Nullable Object value, JsonGenerator generator) {
        switch (value) {
            case null -> generator.writeNull();
            case String string -> generator.writeString(string);
            case byte[] bytes -> generator.writeBinary(bytes);
            case Boolean bool -> generator.writeBoolean(bool);
            case Number number -> generator.writeNumber(number.toString());
            case Map<?, ?> map -> {
                generator.writeStartObject();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    generator.writeName(String.valueOf(entry.getKey()));
                    writeJson(entry.getValue(), generator);
                }
                generator.writeEndObject();
            }
            case List<?> list -> {
                generator.writeStartArray();
                for (Object element : list) {
                    writeJson(element, generator);
                }
                generator.writeEndArray();
            }
            default -> generator.writeString(String.valueOf(value));
        }
    }

    /**
     * Reads the given stored {@code json}, which has to be a single JSON string: the text, or the Base64-decoded text
     * for a {@code byte[]}.
     */
    private static Object readJson(Class<?> type, String json) {
        try (JsonParser parser = JSON_FACTORY.createParser(ObjectReadContext.empty(), json)) {
            if (parser.nextToken() == JsonToken.VALUE_STRING) {
                Object value = type == byte[].class ? parser.getBinaryValue() : parser.getString();
                if (parser.nextToken() == null) {
                    return value;
                }
            }
        } catch (JacksonException e) {
            throw new DeadlineException(INVALID_JSON_STRING, e);
        }
        throw new DeadlineException(INVALID_JSON_STRING);
    }

    /**
     * Ensures XStream is available before {@link XStreamPayloadFormat} is loaded, so that a payload in the XStream
     * format fails with a {@link DeadlineException} instead of a {@link NoClassDefFoundError} when XStream is absent.
     */
    private static void requireXStream() {
        try {
            Class.forName(XSTREAM_CLASS_NAME, false, StoredDeadlineConverter.class.getClassLoader());
        } catch (ClassNotFoundException | LinkageError e) {
            throw new DeadlineException(
                    "The stored payload is XStream XML, which needs XStream on the classpath to be read or written", e
            );
        }
    }

    /**
     * The storage formats in which a {@code String} or {@code byte[]} payload can be written as Axon Framework 4 wrote
     * it.
     */
    private enum StoredFormat {

        /**
         * JSON, as Axon Framework 4's {@code JacksonSerializer} and {@code Jackson3Serializer} wrote it: a JSON string,
         * Base64-encoded for a {@code byte[]}.
         */
        JSON {
            @Override
            String write(Object payload) {
                return writeJson(payload);
            }
        },
        /**
         * XStream XML, as Axon Framework 4's {@code XStreamSerializer} wrote it.
         */
        XSTREAM {
            @Override
            String write(Object payload) {
                requireXStream();
                return XStreamPayloadFormat.write(payload);
            }
        },
        /**
         * Any other format, in which this class does not know how Axon Framework 4 wrote such a payload.
         */
        UNKNOWN {
            @Override
            String write(Object payload) {
                throw new DeadlineException(
                        "A deadline with a payload of type [" + payload.getClass().getTypeName() + "] cannot be "
                                + "stored: the configured Converter writes neither JSON nor XStream XML, the formats "
                                + "in which Axon Framework 4 stored such a payload"
                );
            }
        };

        /**
         * Detects the format of the given {@code converter} from how it writes {@link Boolean#TRUE}: {@code true} in
         * JSON, and {@code <boolean>true</boolean>} in XStream XML.
         */
        private static StoredFormat of(Converter converter) {
            String probe;
            try {
                probe = converter.convert(Boolean.TRUE, String.class);
            } catch (RuntimeException e) {
                return UNKNOWN;
            }
            if ("true".equals(probe)) {
                return JSON;
            }
            return "<boolean>true</boolean>".equals(probe) ? XSTREAM : UNKNOWN;
        }

        abstract String write(Object payload);
    }
}
