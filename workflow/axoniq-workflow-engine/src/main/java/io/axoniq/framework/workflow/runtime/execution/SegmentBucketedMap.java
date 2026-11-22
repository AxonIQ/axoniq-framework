/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A concurrent map keyed by workflow id that lists the entries owned by a {@link Segment} without visiting the entries
 * of other segments.
 * <p>
 * A segment owns the ids whose hash it matches, see {@link WorkflowSegmentOwnership}: the bits of the hash selected by
 * the mask of the segment equal its segment id. The map spreads its entries over {@value #BUCKET_COUNT} buckets on the
 * lowest bits of the hash of their id, so the entries of any segment, whatever its mask, sit in the buckets whose
 * index matches the segment on those bits. Splitting or merging a segment only changes which buckets a lookup visits,
 * never where an entry is stored, so the map holds no state about segments or their claims. A segment whose mask
 * selects more bits than the bucket index covers visits the buckets of its lowest bits and filters their entries.
 * <p>
 * An empty id is rejected when stored: no segment owns it, see
 * {@link WorkflowSegmentOwnership#ownedBy(Segment, String)}, and
 * {@link io.axoniq.framework.workflow.runtime.util.WorkflowIds} rejects it as a workflow id, so it never takes part
 * in a listing.
 * <p>
 * Every bucket is a {@link ConcurrentHashMap}, so the map takes no lock of its own: reads and writes of an entry are as
 * concurrent as those of a {@code ConcurrentHashMap}, and listings are weakly consistent.
 * <p>
 * Internal: an implementation detail of the in-memory indexes of the workflow engine.
 *
 * @param <V> the type of the values
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
final class SegmentBucketedMap<V> {

    /**
     * The number of buckets, a power of two. Lookups of segments of up to this many segments in total visit only the
     * entries they own.
     */
    static final int BUCKET_COUNT = 1024;
    private static final int BUCKET_MASK = BUCKET_COUNT - 1;

    private final List<ConcurrentHashMap<String, V>> buckets =
            IntStream.range(0, BUCKET_COUNT).mapToObj(i -> new ConcurrentHashMap<String, V>()).toList();

    /**
     * Returns the value stored for the given id.
     *
     * @param id the workflow id
     * @return the value, or {@code null} when none is stored
     */
    @Nullable
    V get(String id) {
        return bucketOf(id).get(id);
    }

    /**
     * Stores the value the given function creates for the given id, unless a value is stored for it already. Behaves
     * as {@link ConcurrentHashMap#computeIfAbsent(Object, Function)}.
     *
     * @param id      the workflow id, not empty
     * @param factory the function creating the value
     * @return the stored value
     * @throws IllegalArgumentException when the id is empty
     */
    V computeIfAbsent(String id, Function<String, V> factory) {
        return bucketOf(requireNotEmpty(id)).computeIfAbsent(id, factory);
    }

    /**
     * Stores the given value for the given id, replacing any stored value.
     *
     * @param id    the workflow id, not empty
     * @param value the value
     * @throws IllegalArgumentException when the id is empty
     */
    void put(String id, V value) {
        bucketOf(requireNotEmpty(id)).put(id, value);
    }

    /**
     * Removes the value stored for the given id.
     *
     * @param id the workflow id
     * @return the removed value, or {@code null} when none was stored
     */
    @Nullable
    V remove(String id) {
        return bucketOf(id).remove(id);
    }

    /**
     * Removes the value stored for the given id, but only while it is the given value.
     *
     * @param id    the workflow id
     * @param value the value to remove
     * @return {@code true} when the value was stored and is removed
     */
    boolean remove(String id, V value) {
        return bucketOf(id).remove(id, value);
    }

    /**
     * Removes every entry whose value matches the given predicate. Behaves as
     * {@link java.util.Collection#removeIf(Predicate)} on the values of a {@link ConcurrentHashMap}, per bucket.
     *
     * @param predicate the predicate to match values against
     */
    void removeIf(Predicate<? super V> predicate) {
        buckets.forEach(bucket -> bucket.values().removeIf(predicate));
    }

    /**
     * Returns the values of every entry.
     *
     * @return a weakly consistent stream of all values
     */
    Stream<V> values() {
        return buckets.stream().flatMap(bucket -> bucket.values().stream());
    }

    /**
     * Returns the ids of every entry.
     *
     * @return a weakly consistent stream of all ids
     */
    Stream<String> ids() {
        return buckets.stream().flatMap(bucket -> bucket.keySet().stream());
    }

    /**
     * Returns the entries owned by the given segment, visiting only the buckets that may hold them.
     *
     * @param segment the segment, or {@code null} for every entry, as outside a segmented processor every instance is
     *                in scope
     * @return a weakly consistent stream of the entries owned by the segment
     */
    Stream<Map.Entry<String, V>> entriesOf(@Nullable Segment segment) {
        if (segment == null || segment.getMask() == 0) {
            return buckets.stream().flatMap(bucket -> bucket.entrySet().stream());
        }
        int bucketMask = segment.getMask() & BUCKET_MASK;
        int bucketValue = segment.getSegmentId() & bucketMask;
        Stream<Map.Entry<String, V>> candidates =
                IntStream.range(0, BUCKET_COUNT)
                         .filter(index -> (index & bucketMask) == bucketValue)
                         .mapToObj(buckets::get)
                         .flatMap(bucket -> bucket.entrySet().stream());
        if ((segment.getMask() & ~BUCKET_MASK) == 0) {
            // The bucket index covers every bit of the mask, so every entry of a visited bucket is owned.
            return candidates;
        }
        return candidates.filter(entry -> WorkflowSegmentOwnership.ownedBy(segment, entry.getKey()));
    }

    /**
     * Returns the number of entries.
     *
     * @return the number of entries
     */
    int size() {
        return buckets.stream().mapToInt(ConcurrentHashMap::size).sum();
    }

    /**
     * Removes every entry.
     */
    void clear() {
        buckets.forEach(ConcurrentHashMap::clear);
    }

    private static String requireNotEmpty(String id) {
        if (Objects.requireNonNull(id, "id must not be null").isEmpty()) {
            throw new IllegalArgumentException("A workflow id must not be empty; no segment owns an empty id.");
        }
        return id;
    }

    private ConcurrentHashMap<String, V> bucketOf(String id) {
        Objects.requireNonNull(id, "id must not be null");
        return buckets.get(id.hashCode() & BUCKET_MASK);
    }
}
