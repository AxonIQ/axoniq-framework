/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.bson.Document;
import org.bson.types.Binary;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents an {@link EventMessage} as it is stored within a {@link DeadLetterEntry} in MongoDB.
 * <p>
 * The entry holds the message {@code type}, {@code identifier}, {@code timestamp}, the converted {@code payload} and
 * {@code metadata}, and optionally the aggregate information and tracking token that were present in the
 * {@link org.axonframework.messaging.core.Context} of the dead letter. These are provided and interpreted by a
 * {@link DeadLetterMongoConverter}.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class DeadLetterEventEntry {

    private static final String TYPE_KEY = "type";
    private static final String IDENTIFIER_KEY = "identifier";
    private static final String TIMESTAMP_KEY = "timestamp";
    private static final String PAYLOAD_KEY = "payload";
    private static final String METADATA_KEY = "metadata";
    private static final String AGGREGATE_TYPE_KEY = "aggregateType";
    private static final String AGGREGATE_IDENTIFIER_KEY = "aggregateIdentifier";
    private static final String AGGREGATE_SEQUENCE_NUMBER_KEY = "aggregateSequenceNumber";
    private static final String TOKEN_TYPE_KEY = "tokenType";
    private static final String TOKEN_KEY = "token";

    private final String type;
    private final String identifier;
    private final String timestamp;
    private final byte @Nullable [] payload;
    private final byte @Nullable [] metadata;
    private final @Nullable String aggregateType;
    private final @Nullable String aggregateIdentifier;
    private final @Nullable Long aggregateSequenceNumber;
    private final @Nullable String tokenType;
    private final byte @Nullable [] token;

    /**
     * Creates a {@code DeadLetterEventEntry} based on the {@link Document} as retrieved from MongoDB.
     *
     * @param document the {@link Document} containing the fields of a {@code DeadLetterEventEntry}
     */
    public DeadLetterEventEntry(Document document) {
        this.type = requireNonNull(document.getString(TYPE_KEY), "The event type is required.");
        this.identifier = requireNonNull(document.getString(IDENTIFIER_KEY), "The event identifier is required.");
        this.timestamp = requireNonNull(document.getString(TIMESTAMP_KEY), "The event timestamp is required.");
        this.payload = bytes(document.get(PAYLOAD_KEY));
        this.metadata = bytes(document.get(METADATA_KEY));
        this.aggregateType = document.getString(AGGREGATE_TYPE_KEY);
        this.aggregateIdentifier = document.getString(AGGREGATE_IDENTIFIER_KEY);
        Object sequenceNumber = document.get(AGGREGATE_SEQUENCE_NUMBER_KEY);
        this.aggregateSequenceNumber = sequenceNumber != null ? ((Number) sequenceNumber).longValue() : null;
        this.tokenType = document.getString(TOKEN_TYPE_KEY);
        this.token = bytes(document.get(TOKEN_KEY));
    }

    /**
     * Creates a {@code DeadLetterEventEntry} to store in MongoDB.
     *
     * @param type                    the {@link org.axonframework.messaging.core.MessageType} of the event, in its
     *                                string form
     * @param identifier              the identifier of the event
     * @param timestamp               the timestamp of the event, in its ISO-8601 string form
     * @param payload                 the converted payload of the event
     * @param metadata                the converted metadata of the event
     * @param aggregateType           the aggregate type the event originated from, if any
     * @param aggregateIdentifier     the aggregate identifier the event originated from, if any
     * @param aggregateSequenceNumber the aggregate sequence number of the event, if any
     * @param tokenType               the class name of the tracking token, if any
     * @param token                   the converted tracking token, if any
     */
    @SuppressWarnings("squid:S107")
    public DeadLetterEventEntry(String type,
                                String identifier,
                                String timestamp,
                                byte @Nullable [] payload,
                                byte @Nullable [] metadata,
                                @Nullable String aggregateType,
                                @Nullable String aggregateIdentifier,
                                @Nullable Long aggregateSequenceNumber,
                                @Nullable String tokenType,
                                byte @Nullable [] token) {
        this.type = requireNonNull(type, "The event type should be provided by the DeadLetterMongoConverter.");
        this.identifier = requireNonNull(identifier, "All EventMessage implementations require an identifier.");
        this.timestamp = requireNonNull(timestamp, "All EventMessage implementations require a timestamp.");
        this.payload = payload;
        this.metadata = metadata;
        this.aggregateType = aggregateType;
        this.aggregateIdentifier = aggregateIdentifier;
        this.aggregateSequenceNumber = aggregateSequenceNumber;
        this.tokenType = tokenType;
        this.token = token;
    }

    private static byte @Nullable [] bytes(@Nullable Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Binary binary) {
            return binary.getData();
        }
        return (byte[]) value;
    }

    /**
     * Returns this entry as a MongoDB {@link Document}.
     *
     * @return a {@link Document} representing this entry
     */
    public Document asDocument() {
        Document document = new Document().append(TYPE_KEY, type)
                                          .append(IDENTIFIER_KEY, identifier)
                                          .append(TIMESTAMP_KEY, timestamp)
                                          .append(PAYLOAD_KEY, payload)
                                          .append(METADATA_KEY, metadata);
        if (aggregateType != null) {
            document.append(AGGREGATE_TYPE_KEY, aggregateType);
        }
        if (aggregateIdentifier != null) {
            document.append(AGGREGATE_IDENTIFIER_KEY, aggregateIdentifier);
        }
        if (aggregateSequenceNumber != null) {
            document.append(AGGREGATE_SEQUENCE_NUMBER_KEY, aggregateSequenceNumber);
        }
        if (tokenType != null && token != null) {
            document.append(TOKEN_TYPE_KEY, tokenType).append(TOKEN_KEY, token);
        }
        return document;
    }

    /**
     * Returns the string form of the {@link org.axonframework.messaging.core.MessageType} of the event.
     *
     * @return the string form of the event's type
     */
    public String getType() {
        return type;
    }

    /**
     * Returns the identifier of the event.
     *
     * @return the identifier of the event
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * Returns the timestamp of the event, in its ISO-8601 string form.
     *
     * @return the timestamp of the event
     */
    public String getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the converted payload of the event.
     *
     * @return the converted payload of the event, or {@code null} if there was none
     */
    public byte @Nullable [] getPayload() {
        return payload;
    }

    /**
     * Returns the converted metadata of the event.
     *
     * @return the converted metadata of the event, or {@code null} if there was none
     */
    public byte @Nullable [] getMetadata() {
        return metadata;
    }

    /**
     * Returns the aggregate type the event originated from.
     *
     * @return the aggregate type, or {@code null} if not available
     */
    public @Nullable String getAggregateType() {
        return aggregateType;
    }

    /**
     * Returns the aggregate identifier the event originated from.
     *
     * @return the aggregate identifier, or {@code null} if not available
     */
    public @Nullable String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    /**
     * Returns the sequence number of the event within its aggregate.
     *
     * @return the aggregate sequence number, or {@code null} if not available
     */
    public @Nullable Long getAggregateSequenceNumber() {
        return aggregateSequenceNumber;
    }

    /**
     * Returns the class name of the tracking token that was present when the event was enqueued.
     *
     * @return the tracking token type, or {@code null} if there was no tracking token
     */
    public @Nullable String getTokenType() {
        return tokenType;
    }

    /**
     * Returns the converted tracking token that was present when the event was enqueued.
     *
     * @return the converted tracking token, or {@code null} if there was no tracking token
     */
    public byte @Nullable [] getToken() {
        return token;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeadLetterEventEntry that = (DeadLetterEventEntry) o;
        return Objects.equals(type, that.type)
                && Objects.equals(identifier, that.identifier)
                && Objects.equals(timestamp, that.timestamp)
                && Arrays.equals(payload, that.payload)
                && Arrays.equals(metadata, that.metadata)
                && Objects.equals(aggregateType, that.aggregateType)
                && Objects.equals(aggregateIdentifier, that.aggregateIdentifier)
                && Objects.equals(aggregateSequenceNumber, that.aggregateSequenceNumber)
                && Objects.equals(tokenType, that.tokenType)
                && Arrays.equals(token, that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type,
                            identifier,
                            timestamp,
                            Arrays.hashCode(payload),
                            Arrays.hashCode(metadata),
                            aggregateType,
                            aggregateIdentifier,
                            aggregateSequenceNumber,
                            tokenType,
                            Arrays.hashCode(token));
    }

    @Override
    public String toString() {
        return "DeadLetterEventEntry{" +
                "type='" + type + '\'' +
                ", identifier='" + identifier + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", payload=" + Arrays.toString(payload) +
                ", metadata=" + Arrays.toString(metadata) +
                ", aggregateType='" + aggregateType + '\'' +
                ", aggregateIdentifier='" + aggregateIdentifier + '\'' +
                ", aggregateSequenceNumber=" + aggregateSequenceNumber +
                ", tokenType='" + tokenType + '\'' +
                ", token=" + Arrays.toString(token) +
                '}';
    }
}
