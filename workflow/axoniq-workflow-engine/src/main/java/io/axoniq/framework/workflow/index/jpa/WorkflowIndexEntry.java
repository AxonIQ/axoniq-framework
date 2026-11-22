/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A running workflow instance in the index of a {@link JpaWorkflowIndex}, identified by the name of the index and
 * the workflow id.
 * <p>
 * Next to its id, an entry stores a {@link #segmentKey(String) segment key}: the bit-reversed hash of the workflow id,
 * as an unsigned value. A segment owns the ids whose hash, masked with the segment's mask, equals the segment id; in
 * the bit-reversed hash those masked low bits become the high bits, so the ids a segment owns form one contiguous range
 * of segment keys. A load selects them with a range condition that an index on ({@code indexName}, {@code segmentKey})
 * serves, where a condition on the masked hash would need a bitwise operation JPQL does not offer, and would scan every
 * entry of the index.
 * <p>
 * An entry of a workflow instance that hibernated carries its
 * {@link io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint hibernation hint} in two nullable
 * columns: {@code wakeAt}, the earliest deadline of its open waits, and {@code hibernationWaits}, its open waits as
 * readable JSON, such as
 * {@code [{"step":"awaitPayment","event":"com.acme.PaymentReceived","associations":["payload:orderId=42"]}]}. Both are
 * {@code null} for an instance that is not known to be hibernated.
 * <p>
 * The entity is part of the database schema of the {@link JpaWorkflowIndex}: include it in the persistence unit that
 * index uses. It is not meant to be read or written by application code.
 * <p>
 * The {@code workflowId} column must compare ids case-sensitively and without ignoring trailing spaces, as a binary
 * collation does: the index deletes and inserts entries by workflow id, so under a case-insensitive collation, such
 * as MySQL's default {@code utf8mb4_0900_ai_ci}, the entries of two ids differing only in case would overwrite each
 * other, as would, under a pad-space collation, such as MySQL's {@code utf8mb4_bin}, the entries of two ids differing
 * only in trailing spaces, and a running instance would silently drop out of the index. See {@link JpaWorkflowIndex}
 * for column definitions per database.
 * <p>
 * Marked {@link Internal} as a warning that API stability is not promised yet: this entity is usable, and supported
 * as part of the schema of the {@link JpaWorkflowIndex}, but its shape may still change in a minor release.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
@Entity
@Table(indexes = @Index(columnList = "indexName,segmentKey"))
@IdClass(WorkflowIndexEntry.PK.class)
public class WorkflowIndexEntry {

    private static final long UNSIGNED_INT_RANGE = 1L << Integer.SIZE;

    @Id
    private String indexName;

    @Id
    private String workflowId;

    @Basic(optional = false)
    private long segmentKey;

    @Basic
    @Nullable
    private Instant wakeAt;

    @Column(length = Integer.MAX_VALUE)
    @Nullable
    private String hibernationWaits;

    /**
     * Creates an entry for the given running workflow instance in the index with the given name, with the given
     * hibernation hint.
     *
     * @param indexName  the name of the index the entry belongs to
     * @param workflowId the id of the running workflow instance
     * @param hint       the hint the instance hibernated with, or {@code null} for none
     */
    public WorkflowIndexEntry(String indexName, String workflowId, @Nullable HibernationHint hint) {
        this.indexName = requireNonNull(indexName, "The index name must not be null.");
        this.workflowId = requireNonNull(workflowId, "The workflow id must not be null.");
        this.segmentKey = segmentKey(workflowId);
        if (hint != null) {
            this.wakeAt = hint.wakeAt();
            this.hibernationWaits = HibernationWaitsCodec.encode(hint.waits());
        }
    }

    /**
     * Constructor for JPA.
     */
    protected WorkflowIndexEntry() {
    }

    /**
     * Returns the segment key of the given workflow id: the bit-reversed {@link String#hashCode() hash} of the id, as
     * an unsigned value between {@code 0} and {@code 2^32 - 1}.
     * <p>
     * {@link Segment#matches(Object)} compares the low bits of the hash; reversing the bits makes those the high bits,
     * so the ids a segment owns form one contiguous range of segment keys, see {@link #rangeOf(Segment)}.
     *
     * @param workflowId the id of a workflow instance
     * @return the segment key of the id
     */
    static long segmentKey(String workflowId) {
        return Integer.toUnsignedLong(Integer.reverse(workflowId.hashCode()));
    }

    /**
     * Returns the range of segment keys of the ids the given segment owns.
     * <p>
     * A {@link Segment} always has a mask of the form {@code 2^k - 1}. It owns the ids whose low {@code k} hash bits
     * equal its id, which are the segment keys whose high {@code k} bits equal the bit-reversed segment id. A segment
     * with mask {@code 0} owns every id, and one whose id does not fit within its mask owns none, for which the range
     * is empty.
     *
     * @param segment the segment to select the ids of
     * @return the inclusive range of segment keys the segment owns
     */
    static SegmentKeyRange rangeOf(Segment segment) {
        int mask = segment.getMask();
        int segmentId = segment.getSegmentId();
        if (mask != 0 && (segmentId & ~mask) != 0) {
            return SegmentKeyRange.EMPTY;
        }
        long width = UNSIGNED_INT_RANGE >>> Integer.bitCount(mask);
        long lowest = Integer.toUnsignedLong(Integer.reverse(segmentId & mask));
        return new SegmentKeyRange(lowest, lowest + width - 1);
    }

    /**
     * Returns the name of the index this entry belongs to.
     *
     * @return the name of the index
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Returns the id of the running workflow instance.
     *
     * @return the workflow id
     */
    public String getWorkflowId() {
        return workflowId;
    }

    /**
     * Returns the segment key of the workflow id, see {@link #segmentKey(String)}.
     *
     * @return the segment key
     */
    public long getSegmentKey() {
        return segmentKey;
    }

    /**
     * Returns the earliest deadline of the open waits of the hibernated instance.
     *
     * @return the time the hibernated instance wakes, or {@code null} when it is not known to be hibernated
     */
    public @Nullable Instant getWakeAt() {
        return wakeAt;
    }

    /**
     * Returns the open waits of the hibernated instance, as JSON.
     *
     * @return the JSON text of the open waits, or {@code null} when the instance is not known to be hibernated
     */
    public @Nullable String getHibernationWaits() {
        return hibernationWaits;
    }

    /**
     * Returns the hibernation hint stored in the given column values.
     *
     * @param wakeAt           the value of the {@code wakeAt} column
     * @param hibernationWaits the value of the {@code hibernationWaits} column
     * @return the hint, or {@code null} when either column is {@code null}
     * @throws IllegalArgumentException if the open waits cannot be read
     */
    static @Nullable HibernationHint hint(@Nullable Instant wakeAt, @Nullable String hibernationWaits) {
        if (wakeAt == null || hibernationWaits == null) {
            return null;
        }
        return new HibernationHint(wakeAt, HibernationWaitsCodec.decode(hibernationWaits));
    }

    /**
     * An inclusive range of segment keys.
     *
     * @param lowest  the lowest segment key in the range
     * @param highest the highest segment key in the range
     */
    record SegmentKeyRange(long lowest, long highest) {

        /**
         * A range containing no segment key.
         */
        static final SegmentKeyRange EMPTY = new SegmentKeyRange(1, 0);
    }

    /**
     * The primary key of a {@link WorkflowIndexEntry}: the name of the index and the workflow id.
     */
    public static class PK implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private String indexName;
        private String workflowId;

        /**
         * Constructor for JPA.
         */
        public PK() {
        }

        /**
         * Creates the key of the entry of the given workflow id in the index with the given name.
         *
         * @param indexName  the name of the index
         * @param workflowId the id of the workflow instance
         */
        public PK(String indexName, String workflowId) {
            this.indexName = indexName;
            this.workflowId = workflowId;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof PK other
                    && Objects.equals(indexName, other.indexName)
                    && Objects.equals(workflowId, other.workflowId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, workflowId);
        }
    }
}
