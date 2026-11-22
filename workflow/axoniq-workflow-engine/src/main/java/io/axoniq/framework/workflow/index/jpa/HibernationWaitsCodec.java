/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

import io.axoniq.framework.workflow.index.api.WorkflowIndex.OpenWait;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Writes the open waits of a {@link io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint} as readable
 * JSON, and reads them back, for the {@code hibernationWaits} column of a {@link WorkflowIndexEntry}.
 * <p>
 * The text is a JSON array with one object per wait, so operators can read which events a hibernated workflow waits
 * for:
 * <pre>{@code
 * [{"step":"awaitPayment","event":"com.acme.PaymentReceived","associations":["payload:orderId=42"]}]
 * }</pre>
 * A wait that accepts events of any name has {@code "event":null}. Every character outside printable ASCII is written
 * as a {@code \}{@code uXXXX} escape, so the text is stored unchanged in a column of any character set, also for lone
 * surrogates, which no Unicode encoding can store as is. Reading ignores members it does not know, so a later version
 * may add some, and rejects text nested deeper than a wait can be. It is a codec of its own, so the index needs no JSON
 * library.
 * <p>
 * Marked {@link Internal} as it is an implementation detail of the {@link JpaWorkflowIndex}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
final class HibernationWaitsCodec {

    private static final String STEP = "step";
    private static final String EVENT = "event";
    private static final String ASSOCIATIONS = "associations";
    // Waits nest three levels deep; the margin leaves room for members a later version may add.
    private static final int MAX_DEPTH = 32;

    private HibernationWaitsCodec() {
        // Utility class.
    }

    /**
     * Writes the given waits as a JSON array.
     *
     * @param waits the waits to write
     * @return the JSON text of the waits
     */
    static String encode(List<OpenWait> waits) {
        var json = new StringBuilder("[");
        for (int i = 0; i < waits.size(); i++) {
            var wait = waits.get(i);
            if (i > 0) {
                json.append(',');
            }
            json.append("{\"").append(STEP).append("\":");
            writeString(json, wait.stepName());
            json.append(",\"").append(EVENT).append("\":");
            if (wait.eventName() == null) {
                json.append("null");
            } else {
                writeString(json, wait.eventName());
            }
            json.append(",\"").append(ASSOCIATIONS).append("\":[");
            int j = 0;
            for (var association : wait.associations()) {
                if (j++ > 0) {
                    json.append(',');
                }
                writeString(json, association);
            }
            json.append("]}");
        }
        return json.append(']').toString();
    }

    /**
     * Reads waits from JSON text written by {@link #encode(List)}.
     *
     * @param json the JSON text
     * @return the waits
     * @throws IllegalArgumentException if the text is not a JSON array of waits
     */
    static List<OpenWait> decode(String json) {
        var parser = new Parser(json);
        Object value = parser.readDocument();
        if (!(value instanceof List<?> list)) {
            throw new IllegalArgumentException("The hibernation waits are not a JSON array.");
        }
        var waits = new ArrayList<OpenWait>();
        for (Object element : list) {
            if (!(element instanceof Map<?, ?> wait)) {
                throw new IllegalArgumentException("A hibernation wait is not a JSON object.");
            }
            if (!(wait.get(STEP) instanceof String step)) {
                throw new IllegalArgumentException("A hibernation wait has no step name.");
            }
            Object event = wait.get(EVENT);
            if (event != null && !(event instanceof String)) {
                throw new IllegalArgumentException("The event name of hibernation wait [" + step + "] is no string.");
            }
            Object associations = wait.get(ASSOCIATIONS);
            Set<String> parsedAssociations = new LinkedHashSet<>();
            if (associations instanceof List<?> values) {
                for (Object association : values) {
                    if (!(association instanceof String text)) {
                        throw new IllegalArgumentException(
                                "An association of hibernation wait [" + step + "] is no string."
                        );
                    }
                    parsedAssociations.add(text);
                }
            } else if (associations != null) {
                throw new IllegalArgumentException(
                        "The associations of hibernation wait [" + step + "] are no array."
                );
            }
            waits.add(new OpenWait(step, (String) event, parsedAssociations));
        }
        return waits;
    }

    /**
     * Writes the given text as a JSON string. Escapes quotes, backslashes and control characters as JSON does, and
     * every other character outside printable ASCII as a {@code \}{@code uXXXX} escape of its UTF-16 code unit.
     */
    private static void writeString(StringBuilder json, String text) {
        json.append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                case '\b' -> json.append("\\b");
                case '\f' -> json.append("\\f");
                default -> {
                    if (c < 0x20 || c > 0x7e) {
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
     * Reads JSON arrays, objects, strings and {@code null}: what {@link #encode(List)} writes. Numbers and booleans are
     * read as well, so a member a later version adds does not keep the waits from being read. Arrays and objects nested
     * deeper than {@value HibernationWaitsCodec#MAX_DEPTH} levels are rejected, so corrupt text cannot overflow the
     * stack.
     */
    private static final class Parser {

        private final String json;
        private int position;
        private int depth;

        private Parser(String json) {
            this.json = json;
        }

        Object readDocument() {
            Object value = readValue();
            skipWhitespace();
            if (position != json.length()) {
                throw error("unexpected text after the value");
            }
            return value;
        }

        private @Nullable Object readValue() {
            skipWhitespace();
            if (position >= json.length()) {
                throw error("unexpected end");
            }
            char c = json.charAt(position);
            return switch (c) {
                case '[' -> readArray();
                case '{' -> readObject();
                case '"' -> readString();
                case 'n' -> readLiteral("null", null);
                case 't' -> readLiteral("true", Boolean.TRUE);
                case 'f' -> readLiteral("false", Boolean.FALSE);
                default -> {
                    if (c == '-' || (c >= '0' && c <= '9')) {
                        yield readNumber();
                    }
                    throw error("unexpected character '" + c + "'");
                }
            };
        }

        private List<@Nullable Object> readArray() {
            enter();
            position++;
            var values = new ArrayList<@Nullable Object>();
            skipWhitespace();
            if (peek() == ']') {
                position++;
                depth--;
                return values;
            }
            while (true) {
                values.add(readValue());
                skipWhitespace();
                char c = next();
                if (c == ']') {
                    depth--;
                    return values;
                }
                if (c != ',') {
                    throw error("expected ',' or ']'");
                }
            }
        }

        private Map<String, @Nullable Object> readObject() {
            enter();
            position++;
            var members = new LinkedHashMap<String, @Nullable Object>();
            skipWhitespace();
            if (peek() == '}') {
                position++;
                depth--;
                return members;
            }
            while (true) {
                skipWhitespace();
                if (peek() != '"') {
                    throw error("expected a member name");
                }
                String name = readString();
                skipWhitespace();
                if (next() != ':') {
                    throw error("expected ':'");
                }
                members.put(name, readValue());
                skipWhitespace();
                char c = next();
                if (c == '}') {
                    depth--;
                    return members;
                }
                if (c != ',') {
                    throw error("expected ',' or '}'");
                }
            }
        }

        private String readString() {
            position++;
            var text = new StringBuilder();
            while (true) {
                char c = next();
                if (c == '"') {
                    return text.toString();
                }
                if (c != '\\') {
                    text.append(c);
                    continue;
                }
                char escaped = next();
                switch (escaped) {
                    case '"', '\\', '/' -> text.append(escaped);
                    case 'n' -> text.append('\n');
                    case 'r' -> text.append('\r');
                    case 't' -> text.append('\t');
                    case 'b' -> text.append('\b');
                    case 'f' -> text.append('\f');
                    case 'u' -> text.append(readUnicodeEscape());
                    default -> throw error("invalid escape '\\" + escaped + "'");
                }
            }
        }

        /**
         * Reads the four hex digits of a {@code \}{@code u} escape: exactly four ASCII hex digits, as JSON requires.
         */
        private char readUnicodeEscape() {
            if (position + 4 > json.length()) {
                throw error("incomplete unicode escape");
            }
            int value = 0;
            for (int i = 0; i < 4; i++) {
                int digit = hexDigit(json.charAt(position + i));
                if (digit < 0) {
                    throw error("invalid unicode escape");
                }
                value = value * 16 + digit;
            }
            position += 4;
            return (char) value;
        }

        private static int hexDigit(char c) {
            if (c >= '0' && c <= '9') {
                return c - '0';
            }
            if (c >= 'a' && c <= 'f') {
                return c - 'a' + 10;
            }
            if (c >= 'A' && c <= 'F') {
                return c - 'A' + 10;
            }
            return -1;
        }

        private void enter() {
            if (++depth > MAX_DEPTH) {
                throw error("nested deeper than " + MAX_DEPTH + " levels");
            }
        }

        private @Nullable Object readLiteral(String literal, @Nullable Object value) {
            if (!json.startsWith(literal, position)) {
                throw error("expected " + literal);
            }
            position += literal.length();
            return value;
        }

        private BigDecimal readNumber() {
            int start = position;
            while (position < json.length() && "+-0123456789.eE".indexOf(json.charAt(position)) >= 0) {
                position++;
            }
            try {
                return new BigDecimal(json.substring(start, position));
            } catch (NumberFormatException e) {
                throw error("invalid number");
            }
        }

        private void skipWhitespace() {
            while (position < json.length() && Character.isWhitespace(json.charAt(position))) {
                position++;
            }
        }

        private char peek() {
            if (position >= json.length()) {
                throw error("unexpected end");
            }
            return json.charAt(position);
        }

        private char next() {
            char c = peek();
            position++;
            return c;
        }

        private IllegalArgumentException error(String problem) {
            return new IllegalArgumentException(
                    "Cannot read the hibernation waits at position " + position + ": " + problem + "."
            );
        }
    }
}
