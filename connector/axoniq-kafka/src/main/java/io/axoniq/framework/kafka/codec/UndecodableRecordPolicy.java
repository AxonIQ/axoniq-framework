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

package io.axoniq.framework.kafka.codec;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Decides what happens to a consumed Kafka record that does not yield an event.
 * <p>
 * The Kafka event sources apply this policy to records that carry no event according to the configured
 * {@link KafkaEventCodec}, such as records another application wrote, and to malformed records: a key or value that
 * cannot be deserialized, or a record the codec rejects with an {@link UndecodableRecordException}. A skipped record
 * counts as consumed: the consumer's position moves past it.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public enum UndecodableRecordPolicy {

    /**
     * Skips the record and logs a warning that names the record's topic, partition and offset, and the reason it could
     * not be decoded, when there is one. This is the default.
     */
    SKIP_AND_LOG {
        @Override
        public void handle(TopicPartition partition, long offset, @Nullable Throwable cause) {
            if (cause == null) {
                logger.warn("Skipping the record at offset [{}] of partition [{}] on topic [{}]: it does not carry "
                                    + "an event in the format of the configured codec.",
                            offset, partition.partition(), partition.topic());
            } else {
                logger.warn("Skipping the record at offset [{}] of partition [{}] on topic [{}]: it cannot be "
                                    + "decoded.",
                            offset, partition.partition(), partition.topic(), cause);
            }
        }
    },

    /**
     * Rejects the record with an {@link UndecodableRecordException}, so that consumption stops at that record until it
     * is dealt with, for example by fixing the codec or by moving the consumer past the record.
     * <p>
     * The {@link io.axoniq.framework.kafka.streaming.KafkaStreamableEventSource} fails the event stream with the
     * exception; the processor reading it reopens the stream at that record when it retries. The
     * {@link io.axoniq.framework.kafka.subscribing.KafkaSubscribableEventSource} stops consuming the record's partition
     * at that record, whatever its {@link io.axoniq.framework.kafka.subscribing.FailedRecordPolicy}: the partition is
     * paused, offsets are never committed past the record, and the partition is consumed again from that record when it
     * is assigned again, for example after a restart. Its other partitions are not affected.
     */
    FAIL {
        @Override
        public void handle(TopicPartition partition, long offset, @Nullable Throwable cause) {
            String record = "The record at offset [" + offset + "] of partition [" + partition.partition()
                    + "] on topic [" + partition.topic() + "] ";
            if (cause == null) {
                throw new UndecodableRecordException(
                        record + "does not carry an event in the format of the configured codec."
                );
            }
            throw new UndecodableRecordException(record + "cannot be decoded: " + cause.getMessage(), cause);
        }
    };

    private static final Logger logger = LoggerFactory.getLogger(UndecodableRecordPolicy.class);

    /**
     * Decodes the given {@code record} with the given {@code codec}, applying this policy when the record yields no
     * event. Any exception the codec throws makes the record undecodable.
     * <p>
     * Internal, because the Kafka event sources call it for every record they consume; applications choose a policy,
     * but do not apply it themselves.
     *
     * @param codec  the codec that decodes the record
     * @param record the record to decode
     * @param <V>    the type of record value
     * @return the decoded record, or an empty {@link Optional} when this policy skipped the record
     * @throws UndecodableRecordException if this policy rejects the record
     */
    @Internal
    public <V> Optional<DecodedRecord> decode(KafkaEventCodec<V> codec, ConsumerRecord<String, V> record) {
        Optional<DecodedRecord> decoded;
        try {
            decoded = codec.decode(record);
        } catch (RuntimeException e) {
            handle(new TopicPartition(record.topic(), record.partition()), record.offset(), e);
            return Optional.empty();
        }
        if (decoded.isEmpty()) {
            handle(new TopicPartition(record.topic(), record.partition()), record.offset(), null);
        }
        return decoded;
    }

    /**
     * Applies this policy to the record at the given {@code offset} of the given {@code partition}, which yields no
     * event. It also serves records that are not available as a {@link ConsumerRecord}, such as a record whose key or
     * value cannot be deserialized.
     * <p>
     * Internal, because the Kafka event sources apply the policy themselves; applications choose a policy, but do not
     * apply it themselves.
     *
     * @param partition the partition of the record
     * @param offset    the offset of the record
     * @param cause     the reason the record could not be decoded, or {@code null} when it simply carries no event
     * @throws UndecodableRecordException if this policy rejects the record
     */
    @Internal
    public abstract void handle(TopicPartition partition, long offset, @Nullable Throwable cause);
}
