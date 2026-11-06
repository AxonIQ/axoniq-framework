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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BinaryOperator;

/**
 * A {@link TrackingToken} describing a position in one or more Kafka topics, as the offset to continue from in each
 * partition.
 * <p>
 * <b>Offset convention:</b> for every partition, the token holds the offset of the <em>next record to read</em>, not
 * the offset of the last record that was read. After consuming the record at offset {@code 41} of partition
 * {@code parcels-0}, the token holds {@code parcels-0 = 42}. This is Kafka's own convention for consumer positions and
 * committed offsets. A partition the token does not mention is read from its earliest available record. A partition it
 * mentions is read from the token's offset, also when that is {@code 0}: when Kafka has deleted records at or after
 * that offset under the topic's retention settings, the
 * {@link io.axoniq.framework.kafka.common.RetentionGapPolicy} applies.
 * <p>
 * When tokens are combined, through {@link #lowerBound} or a segment merge, a partition that only some of them mention
 * is read from its earliest available record: records of it that retention deleted are then skipped without the
 * retention gap policy applying. Equality treats an offset of {@code 0} as an unmentioned partition. Records that still
 * exist are never skipped.
 * <p>
 * Tokens stored by Axon Framework 4's Kafka extension are read as {@link LegacyKafkaTrackingToken}.
 * <p>
 * Tokens compare per partition: a token {@link #covers covers} another when it is at or beyond the other's offset in
 * every partition. The {@link #position() position} is the sum of all offsets, which serves to estimate progress and
 * lag. The lower bound of two tokens leaves out the partitions that either token does not mention, as that token reads
 * them from their earliest available record. Bounds can only be taken with other Kafka tokens,
 * {@link TrackingToken#FIRST} and {@link TrackingToken#LATEST}; a token of another type is rejected with an
 * {@link IllegalArgumentException}, and is never covered.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public final class KafkaTrackingToken implements TrackingToken {

    private static final KafkaTrackingToken EMPTY = create(Map.of());

    private final transient Map<TopicPartition, Long> positions;
    private transient volatile @Nullable Map<String, Long> serializedForm;

    /**
     * Constructor for deserialization, taking the offsets keyed by the {@link TopicPartition#toString() string form}
     * of each partition, such as {@code parcels-0}.
     *
     * @param serializedPositions the next offset to read for each partition, keyed by the partition's string form
     * @throws IllegalArgumentException if an offset is negative, or a key is not a topic partition
     */
    @JsonCreator
    @ConstructorProperties({"positions"})
    public KafkaTrackingToken(@JsonProperty("positions") Map<String, Long> serializedPositions) {
        this.positions = validated(parse(Objects.requireNonNull(serializedPositions,
                                                                "The positions must not be null.")));
    }

    /**
     * Constructs a token from positions that are validated and immutable already. The {@code unused} parameter only
     * sets this constructor apart from the public one, whose parameter has the same erasure.
     */
    private KafkaTrackingToken(Map<TopicPartition, Long> validatedPositions, boolean unused) {
        this.positions = validatedPositions;
    }

    private static KafkaTrackingToken create(Map<TopicPartition, Long> positions) {
        return new KafkaTrackingToken(validated(positions), true);
    }

    private static Map<TopicPartition, Long> validated(Map<TopicPartition, Long> positions) {
        positions.forEach((partition, offset) -> {
            if (offset < 0) {
                throw new IllegalArgumentException("Offset of partition " + partition + " must not be negative.");
            }
        });
        return Map.copyOf(positions);
    }

    /**
     * Returns the offsets keyed by the {@link TopicPartition#toString() string form} of each partition, sorted, as
     * they are serialized. Computed on first use, as most tokens are never serialized.
     *
     * @return the next offset to read for each partition, keyed by the partition's string form
     */
    @JsonProperty("positions")
    private Map<String, Long> serializedPositions() {
        Map<String, Long> result = serializedForm;
        if (result == null) {
            Map<String, Long> sorted = new TreeMap<>();
            positions.forEach((partition, offset) -> sorted.put(partition.toString(), offset));
            result = Collections.unmodifiableMap(sorted);
            serializedForm = result;
        }
        return result;
    }

    /**
     * Returns a token with no offsets, positioned at the beginning of every partition.
     *
     * @return a token positioned at the beginning of every partition
     */
    public static KafkaTrackingToken empty() {
        return EMPTY;
    }

    /**
     * Returns a token holding the given {@code positions}: the offset of the next record to read for each
     * partition.
     *
     * @param positions the next offset to read for each partition
     * @return a token holding the given {@code positions}
     * @throws IllegalArgumentException if an offset is negative
     */
    public static KafkaTrackingToken of(Map<TopicPartition, Long> positions) {
        return create(Objects.requireNonNull(positions, "The positions must not be null."));
    }

    private static Map<TopicPartition, Long> parse(Map<String, Long> serializedPositions) {
        Map<TopicPartition, Long> result = new HashMap<>();
        serializedPositions.forEach((key, offset) -> {
            int separator = key.lastIndexOf('-');
            if (separator <= 0) {
                throw new IllegalArgumentException("Not a topic partition: [" + key + "].");
            }
            result.put(new TopicPartition(key.substring(0, separator), Integer.parseInt(key.substring(separator + 1))),
                       offset);
        });
        return result;
    }

    /**
     * Returns the next offset to read for each partition this token mentions.
     *
     * @return the next offset to read for each partition, as an unmodifiable map
     */
    public Map<TopicPartition, Long> positions() {
        return positions;
    }

    /**
     * Returns the next offset to read in the given {@code partition}, which is {@code 0} when this token does not
     * mention the partition.
     *
     * @param partition the partition to return the offset for
     * @return the next offset to read in the given {@code partition}
     */
    public long offsetOf(TopicPartition partition) {
        return positions.getOrDefault(partition, 0L);
    }

    /**
     * Returns a token that is advanced to the given {@code nextOffset} in the given {@code partition}, and equal to
     * this token in all other partitions.
     *
     * @param partition  the partition to advance
     * @param nextOffset the offset of the next record to read in the partition
     * @return a token advanced to the given {@code nextOffset} in the given {@code partition}
     */
    public KafkaTrackingToken advancedTo(TopicPartition partition, long nextOffset) {
        Map<TopicPartition, Long> advanced = new HashMap<>(positions);
        advanced.put(partition, nextOffset);
        return create(advanced);
    }

    @Override
    public TrackingToken lowerBound(TrackingToken other) {
        other = LegacyKafkaTrackingToken.normalize(other);
        if (other instanceof KafkaTrackingToken kafkaToken) {
            return combine(kafkaToken, Math::min, false);
        }
        if (isBoundary(other)) {
            return other.lowerBound(this);
        }
        throw incompatible(other);
    }

    @Override
    public TrackingToken upperBound(TrackingToken other) {
        other = LegacyKafkaTrackingToken.normalize(other);
        if (other instanceof KafkaTrackingToken kafkaToken) {
            return combine(kafkaToken, Math::max, true);
        }
        if (isBoundary(other)) {
            return other.upperBound(this);
        }
        throw incompatible(other);
    }

    /**
     * Returns whether the given {@code token} is {@link TrackingToken#FIRST} or {@link TrackingToken#LATEST}, which
     * bound any token without delegating back.
     */
    private static boolean isBoundary(TrackingToken token) {
        return TrackingToken.FIRST.equals(token) || TrackingToken.LATEST.equals(token);
    }

    private static IllegalArgumentException incompatible(TrackingToken other) {
        return new IllegalArgumentException("Incompatible token type provided: " + other.getClass().getSimpleName());
    }

    /**
     * Combines the positions of this token and the {@code other} with the given {@code operator}, over the partitions
     * that both tokens mention, or, when {@code anyToken} is set, over those that either mentions, a missing position
     * counting as {@code 0}.
     */
    private KafkaTrackingToken combine(KafkaTrackingToken other, BinaryOperator<Long> operator, boolean anyToken) {
        Set<TopicPartition> partitions = new HashSet<>(positions.keySet());
        if (anyToken) {
            partitions.addAll(other.positions.keySet());
        } else {
            partitions.retainAll(other.positions.keySet());
        }
        Map<TopicPartition, Long> combined = new HashMap<>();
        partitions.forEach(p -> combined.put(p, operator.apply(offsetOf(p), other.offsetOf(p))));
        return create(combined);
    }

    @Override
    public boolean covers(@Nullable TrackingToken other) {
        if (other == null || TrackingToken.FIRST.equals(other)) {
            return true;
        }
        if (!(LegacyKafkaTrackingToken.normalize(other) instanceof KafkaTrackingToken kafkaToken)) {
            return false;
        }
        return kafkaToken.positions.entrySet().stream().allMatch(e -> offsetOf(e.getKey()) >= e.getValue());
    }

    @Override
    public OptionalLong position() {
        return OptionalLong.of(positions.values().stream().mapToLong(Long::longValue).sum());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaTrackingToken that)) {
            return false;
        }
        // A partition at offset 0 is the same progress as a partition that is not mentioned: no record of it was read.
        // They are read differently, though: only a mentioned partition is subject to the retention gap policy.
        Set<TopicPartition> partitions = new HashSet<>(positions.keySet());
        partitions.addAll(that.positions.keySet());
        return partitions.stream().allMatch(p -> offsetOf(p) == that.offsetOf(p));
    }

    @Override
    public int hashCode() {
        return positions.entrySet().stream()
                        .filter(e -> e.getValue() != 0L)
                        .mapToInt(e -> e.getKey().hashCode() ^ Long.hashCode(e.getValue()))
                        .sum();
    }

    @Override
    public String toString() {
        return "KafkaTrackingToken" + serializedPositions();
    }
}
