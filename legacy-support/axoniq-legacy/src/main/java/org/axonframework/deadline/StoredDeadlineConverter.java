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
import java.util.Base64;
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
 * A {@code String} or {@code byte[]} payload is stored the way Axon Framework 4's serializers stored it: as a JSON
 * string, Base64-encoded for a {@code byte[]}, or as an XStream {@code <string>} or {@code <byte-array>} element. A
 * {@link Converter} treats a {@code String} or {@code byte[]} as content that is already in its stored form, so it
 * would store such a payload as is. Which of the two forms to write follows from the configured converter's format,
 * which is detected once, by converting a probe value. A converter that writes neither JSON nor XStream XML cannot
 * store such a payload, and scheduling one fails. Reading follows the stored form, and also accepts the type names
 * {@code string} and {@code byte-array}, under which Axon Framework 4's {@code XStreamSerializer} stored these
 * payloads.
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
            String text = readJsonString(trimmed);
            value = type == byte[].class ? Base64.getDecoder().decode(text) : text;
        } else if (trimmed.startsWith("<")) {
            value = readXmlElement(type, trimmed);
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

    private static String readJsonString(String json) {
        if (json.length() < 2 || !json.endsWith("\"")) {
            throw new DeadlineException("The stored payload is no complete JSON string");
        }
        StringBuilder text = new StringBuilder(json.length());
        for (int i = 1; i < json.length() - 1; i++) {
            char c = json.charAt(i);
            if (c != '\\') {
                text.append(c);
                continue;
            }
            char escaped = json.charAt(++i);
            switch (escaped) {
                case 'b' -> text.append('\b');
                case 'f' -> text.append('\f');
                case 'n' -> text.append('\n');
                case 'r' -> text.append('\r');
                case 't' -> text.append('\t');
                case 'u' -> {
                    text.append((char) Integer.parseInt(json.substring(i + 1, i + 5), 16));
                    i += 4;
                }
                default -> text.append(escaped);
            }
        }
        return text.toString();
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
    /**
     * Reads the XStream {@code <string>} or {@code <byte-array>} element matching the given {@code type}: its text, or
     * its Base64-decoded text for a {@code byte[]}. Returns {@code null} if the stored XML is no such element.
     */
    private static @Nullable Object readXmlElement(Class<?> type, String xml) {
        String element = type == byte[].class ? "byte-array" : "string";
        String start = "<" + element + ">";
        String end = "</" + element + ">";
        String text;
        if (xml.equals("<" + element + "/>")) {
            text = "";
        } else if (xml.length() >= start.length() + end.length() && xml.startsWith(start) && xml.endsWith(end)) {
            text = readXmlText(xml.substring(start.length(), xml.length() - end.length()));
        } else {
            return null;
        }
        // The MIME decoder skips line breaks, which XStream's own Base64 encoding may insert.
        return type == byte[].class ? Base64.getMimeDecoder().decode(text) : text;
    }

    private static String readXmlText(String xml) {
        StringBuilder text = new StringBuilder(xml.length());
        for (int i = 0; i < xml.length(); i++) {
            char c = xml.charAt(i);
            if (c != '&') {
                text.append(c);
                continue;
            }
            int end = xml.indexOf(';', i);
            if (end < 0) {
                throw new DeadlineException("The stored payload holds an incomplete XML entity");
            }
            String entity = xml.substring(i + 1, end);
            switch (entity) {
                case "amp" -> text.append('&');
                case "lt" -> text.append('<');
                case "gt" -> text.append('>');
                case "quot" -> text.append('"');
                case "apos" -> text.append('\'');
                default -> text.appendCodePoint(characterReference(entity));
            }
            i = end;
        }
        return text.toString();
    }

    private static int characterReference(String entity) {
        try {
            if (entity.startsWith("#x")) {
                return Integer.parseInt(entity.substring(2), 16);
            }
            if (entity.startsWith("#")) {
                return Integer.parseInt(entity.substring(1));
            }
        } catch (NumberFormatException e) {
            // Reported below, as for any other entity this class does not know.
        }
        throw new DeadlineException("The stored payload holds the unknown XML entity [&" + entity + ";]");
    }

    private static void writeXmlText(String value, StringBuilder xml) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '&' -> xml.append("&amp;");
                case '<' -> xml.append("&lt;");
                case '>' -> xml.append("&gt;");
                case '"' -> xml.append("&quot;");
                case '\'' -> xml.append("&apos;");
                case '\r' -> xml.append("&#xd;");
                default -> {
                    if (c < 0x20 && c != '\t' && c != '\n') {
                        xml.append("&#x").append(Integer.toHexString(c)).append(';');
                    } else {
                        xml.append(c);
                    }
                }
            }
        }
    }

    /**
     * The storage formats in which a {@code String} or {@code byte[]} payload can be written as Axon Framework 4 wrote
     * it.
     */
    private enum StoredFormat {

        /**
         * JSON, as Axon Framework 4's {@code JacksonSerializer} and {@code Jackson3Serializer} wrote it.
         */
        JSON {
            @Override
            String write(Object payload) {
                StringBuilder json = new StringBuilder();
                writeJsonString(payload instanceof byte[] bytes
                                        ? Base64.getEncoder().encodeToString(bytes)
                                        : (String) payload, json);
                return json.toString();
            }
        },
        /**
         * XStream XML, as Axon Framework 4's {@code XStreamSerializer} wrote it.
         */
        XSTREAM {
            @Override
            String write(Object payload) {
                if (payload instanceof byte[] bytes) {
                    return "<byte-array>" + Base64.getEncoder().encodeToString(bytes) + "</byte-array>";
                }
                StringBuilder xml = new StringBuilder("<string>");
                writeXmlText((String) payload, xml);
                return xml.append("</string>").toString();
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
