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

package io.axoniq.framework.kafka.subscribing;

import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * A partition that a {@link KafkaSubscribableEventSource} owns, but does not progress on, as reported by
 * {@link KafkaSourceStatus.Running#stoppedPartitions()}: where it stopped, why, and the failure that made it stop.
 * <p>
 * The source creates these; applications read them, for example to report them in a health check.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public final class StoppedPartition {

    private final TopicPartition partition;
    private final long offset;
    private final Reason reason;
    private final @Nullable Throwable cause;
    private final int attempts;

    /**
     * Why a partition does not progress.
     */
    public enum Reason {

        /**
         * The {@link io.axoniq.framework.kafka.codec.UndecodableRecordPolicy#FAIL} policy rejected the record at the
         * offset. The partition stays stopped until it is assigned again, for example after a restart.
         */
        UNDECODABLE_RECORD,

        /**
         * The committed offset no longer exists in the partition, for example because Kafka deleted its records under
         * the topic's retention settings, and the {@link io.axoniq.framework.kafka.common.RetentionGapPolicy#FAIL}
         * policy applies. The partition stays stopped until the consumer group's offsets are reset.
         */
        RETENTION_GAP,

        /**
         * Processing the event of the record at the offset failed, and the {@link FailedRecordPolicy#RETRY} policy
         * retries it, for as long as it fails. Also reported under the {@link FailedRecordPolicy#DEAD_LETTER_TOPIC}
         * policy while writing the record to its dead-letter topic fails.
         */
        RETRYING
    }

    /**
     * Constructs a report of the given {@code partition}, stopped at the given {@code offset} for the given
     * {@code reason}.
     *
     * @param partition the partition that does not progress
     * @param offset    the offset of the record the partition stopped at
     * @param reason    why the partition does not progress
     * @param cause     the failure that made the partition stop, or {@code null} when there is none
     * @param attempts  the number of failed attempts to process the record, for {@link Reason#RETRYING}, or 0
     */
    StoppedPartition(TopicPartition partition,
                     long offset,
                     Reason reason,
                     @Nullable Throwable cause,
                     int attempts) {
        this.partition = Objects.requireNonNull(partition, "The partition must not be null.");
        this.offset = offset;
        this.reason = Objects.requireNonNull(reason, "The reason must not be null.");
        this.cause = cause;
        this.attempts = attempts;
    }

    /**
     * Returns the partition that does not progress.
     *
     * @return the partition that does not progress
     */
    public TopicPartition partition() {
        return partition;
    }

    /**
     * Returns the offset of the record the partition stopped at: the rejected or retried record, or the committed
     * offset that no longer exists.
     *
     * @return the offset the partition stopped at
     */
    public long offset() {
        return offset;
    }

    /**
     * Returns why the partition does not progress.
     *
     * @return why the partition does not progress
     */
    public Reason reason() {
        return reason;
    }

    /**
     * Returns the failure that made the partition stop: the rejection of an undecodable record, the failure of the
     * last attempt to process a retried record, or the exception that reported a retention gap.
     *
     * @return the failure that made the partition stop, or {@code null} when there is none
     */
    public @Nullable Throwable cause() {
        return cause;
    }

    /**
     * Returns the number of failed attempts to process the record, for a partition whose record is
     * {@link Reason#RETRYING retried}.
     *
     * @return the number of failed attempts, or 0 for the other reasons
     */
    public int attempts() {
        return attempts;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof StoppedPartition that
                && offset == that.offset
                && attempts == that.attempts
                && partition.equals(that.partition)
                && reason == that.reason
                && Objects.equals(cause, that.cause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, offset, reason, cause, attempts);
    }

    @Override
    public String toString() {
        return "StoppedPartition{partition=" + partition + ", offset=" + offset + ", reason=" + reason
                + ", attempts=" + attempts + ", cause=" + cause + "}";
    }
}
