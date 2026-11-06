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

package io.axoniq.framework.kafka.streaming;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.beans.ConstructorProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;

/**
 * A token stored by the Kafka extension of Axon Framework 4, read back so that a processor migrated to Axoniq
 * Framework resumes exactly where it stopped.
 * <p>
 * Axon Framework 4 stored, for every partition, the offset of the <em>last consumed record</em>, where
 * {@link KafkaTrackingToken} holds the offset of the <em>next record to read</em>. This token holds the stored
 * values unchanged, and behaves as the equivalent {@link KafkaTrackingToken}, one offset further in every partition.
 * Stored again, it keeps the original values, so that it can be read back any number of times without moving. Once
 * the processor handles an event, its token is a {@link KafkaTrackingToken}.
 * <p>
 * A legacy token is never {@link #equals(Object) equal} to its equivalent {@link KafkaTrackingToken}, as it stores
 * different values, but the two {@link #samePositionAs(TrackingToken) are at the same position} and
 * {@link #covers(TrackingToken) cover} each other.
 * <p>
 * Token stores read these tokens through the {@link KafkaLegacyTokenTypeMapper}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public final class LegacyKafkaTrackingToken implements TrackingToken {

    @JsonProperty("positions")
    private final Map<String, Long> lastConsumed;
    private final transient KafkaTrackingToken equivalent;

    /**
     * Constructs a token from the offsets Axon Framework 4 stored: the offset of the last consumed record of every
     * partition, keyed by the partition's string form, such as {@code parcels-0}.
     *
     * @param lastConsumed the offset of the last consumed record of every partition
     */
    @JsonCreator
    @ConstructorProperties({"positions"})
    public LegacyKafkaTrackingToken(@JsonProperty("positions") Map<String, Long> lastConsumed) {
        this.lastConsumed = new TreeMap<>(Objects.requireNonNull(lastConsumed, "The positions must not be null."));
        Map<String, Long> nextOffsets = new HashMap<>();
        lastConsumed.forEach((partition, offset) -> nextOffsets.put(partition, offset + 1));
        this.equivalent = new KafkaTrackingToken(nextOffsets);
    }

    /**
     * Returns the {@link KafkaTrackingToken} this token is equivalent to: one offset further in every partition.
     *
     * @return the equivalent token, holding the offset of the next record to read in every partition
     */
    public KafkaTrackingToken toKafkaTrackingToken() {
        return equivalent;
    }

    /**
     * Returns the offset of the last consumed record of every partition, as Axon Framework 4 stored it.
     *
     * @return the stored offsets, by partition
     */
    public Map<TopicPartition, Long> lastConsumedOffsets() {
        Map<TopicPartition, Long> result = new HashMap<>();
        equivalent.positions().forEach((partition, next) -> result.put(partition, next - 1));
        return result;
    }

    @Override
    public TrackingToken lowerBound(TrackingToken other) {
        return equivalent.lowerBound(normalize(other));
    }

    @Override
    public TrackingToken upperBound(TrackingToken other) {
        return equivalent.upperBound(normalize(other));
    }

    @Override
    public boolean covers(@Nullable TrackingToken other) {
        return equivalent.covers(other == null ? null : normalize(other));
    }

    @Override
    public OptionalLong position() {
        return equivalent.position();
    }

    /**
     * Returns the given {@code token}, replacing a legacy token by its {@link KafkaTrackingToken} equivalent.
     *
     * @param token the token to normalize
     * @return the given token, or its {@link KafkaTrackingToken} equivalent when it is a legacy token
     */
    static TrackingToken normalize(TrackingToken token) {
        return token instanceof LegacyKafkaTrackingToken legacy ? legacy.equivalent : token;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof LegacyKafkaTrackingToken that && lastConsumed.equals(that.lastConsumed));
    }

    @Override
    public int hashCode() {
        return lastConsumed.hashCode();
    }

    @Override
    public String toString() {
        return "LegacyKafkaTrackingToken" + lastConsumed;
    }
}
