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

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
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
 * Metadata is read untyped, into a {@code Map<String, Object>}, and each value is turned into a {@code String}: Axon
 * Framework 4 metadata could hold any value, while Axon Framework 5 metadata holds strings only. Strings stay as they
 * are, numbers and booleans become their {@link String#valueOf(Object) string value}, and maps and lists are rendered
 * as JSON by a fixed writer, so the form does not depend on the converter's storage format.
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
    private static final TypeReference<LinkedHashMap<String, Object>> UNTYPED_MAP = new TypeReference<>() {
    };
    private static final MessageTypeResolver MESSAGE_TYPE_RESOLVER = new ClassBasedMessageTypeResolver();

    private final Converter converter;

    /**
     * Creates a {@code StoredDeadlineConverter} converting with the given {@code converter}.
     *
     * @param converter the converter matching the stored form of the deadlines
     */
    public StoredDeadlineConverter(Converter converter) {
        this.converter = Objects.requireNonNull(converter, "The Converter may not be null.");
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
     * {@value #EMPTY_TYPE}, but reject a job whose data is missing.
     *
     * @param payload        the payload of the deadline to store, if any
     * @param representation the stored representation, {@code byte[]} or {@code String}
     * @param <T>            the stored representation
     * @return the stored form of the given {@code payload}
     */
    public <T> @Nullable T payloadToStored(@Nullable Object payload, Class<T> representation) {
        if (payload != null) {
            return converter.convert(payload, representation);
        }
        if (representation == byte[].class) {
            return representation.cast(SERIALIZED_NULL.getBytes(StandardCharsets.UTF_8));
        }
        return representation.cast(SERIALIZED_NULL);
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
     */
    public @Nullable Object payload(String typeName, @Nullable String revision, @Nullable Object data) {
        if (EMPTY_TYPE.equals(typeName)) {
            return null;
        }
        Optional<Class<?>> payloadType = resolve(typeName);
        if (payloadType.isEmpty()) {
            return new UnknownDeadlinePayload(typeName, revision, data);
        }
        return converter.convert(data, payloadType.get());
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
        Map<String, Object> untyped = converter.convert(data, UNTYPED_MAP.getType());
        if (untyped == null) {
            return Metadata.emptyInstance();
        }
        Map<String, String> metadata = new LinkedHashMap<>();
        untyped.forEach((key, value) -> metadata.put(key, asString(value)));
        return Metadata.from(metadata);
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
            StringBuilder json = new StringBuilder();
            writeJson(value, json);
            return json.toString();
        }
        return String.valueOf(value);
    }

    private static void writeJson(@Nullable Object value, StringBuilder json) {
        if (value == null) {
            json.append("null");
        } else if (value instanceof String string) {
            writeJsonString(string, json);
        } else if (value instanceof Number || value instanceof Boolean) {
            json.append(value);
        } else if (value instanceof Map<?, ?> map) {
            json.append('{');
            Iterator<? extends Map.Entry<?, ?>> entries = map.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<?, ?> entry = entries.next();
                writeJsonString(String.valueOf(entry.getKey()), json);
                json.append(':');
                writeJson(entry.getValue(), json);
                if (entries.hasNext()) {
                    json.append(',');
                }
            }
            json.append('}');
        } else if (value instanceof List<?> list) {
            json.append('[');
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    json.append(',');
                }
                writeJson(list.get(i), json);
            }
            json.append(']');
        } else {
            writeJsonString(String.valueOf(value), json);
        }
    }

    private static void writeJsonString(String value, StringBuilder json) {
        json.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\b' -> json.append("\\b");
                case '\f' -> json.append("\\f");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (c < 0x20) {
                        json.append(String.format("\\u%04x", (int) c));
                    } else {
                        json.append(c);
                    }
                }
            }
        }
        json.append('"');
    }
}
