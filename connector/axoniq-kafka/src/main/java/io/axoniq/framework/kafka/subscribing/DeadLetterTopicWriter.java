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

import io.axoniq.framework.kafka.common.KafkaClientFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Writes failed records to dead-letter topics, for the {@link FailedRecordPolicy#DEAD_LETTER_TOPIC} policy of a
 * {@link KafkaSubscribableEventSource}.
 * <p>
 * A dead-letter record is written to the topic named after the original record's topic with the configured suffix. It
 * keeps the original record's key, value, timestamp and headers, and adds the {@link DeadLetterHeaders} describing the
 * failure and the original record's position.
 * <p>
 * The writer owns its producer: it creates it when the first record is written, and closes it when the source stops.
 * Once {@link #close(Duration) closed}, it writes no more records. All consumers of one source share one writer.
 * <p>
 * Internal, because it is created by and only reachable through {@link KafkaSubscribableEventSource}.
 *
 * @param <V> the type of record value
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
class DeadLetterTopicWriter<V> {

    private static final Logger logger = LoggerFactory.getLogger(DeadLetterTopicWriter.class);
    /**
     * The client name of the dead-letter producer, see {@link KafkaClientFactory#forClient(String)}.
     */
    private static final String CLIENT_NAME = "dlt";

    private final Supplier<Producer<String, V>> producerFactory;
    private final String suffix;
    private @Nullable Producer<String, V> producer;
    private boolean closed;

    /**
     * Constructs a writer that writes with a producer from the given {@code producerFactory}, to the topics named
     * after the original records' topics with the given {@code suffix}.
     *
     * @param producerFactory creates the producer, once, when the first record is written
     * @param suffix          the suffix that names a topic's dead-letter topic
     */
    DeadLetterTopicWriter(Supplier<Producer<String, V>> producerFactory, String suffix) {
        this.producerFactory = producerFactory;
        this.suffix = suffix;
    }

    /**
     * Creates a writer whose producer the given {@code clientFactory} creates, for the client name {@code dlt}, with
     * the given {@code producerProperties}. Records are written from the consumers' polling threads, which must not
     * block for long, so the producer's {@code max.block.ms} is {@link ShutdownBudget#DEAD_LETTER_MAX_BLOCK} unless the
     * properties set it.
     *
     * @param clientFactory      the factory creating the producer
     * @param producerProperties the settings of the producer
     * @param valueSerializer    the serializer of the record values
     * @param suffix             the suffix that names a topic's dead-letter topic
     * @param <V>                the type of record value
     * @return a writer that creates its producer when the first record is written
     */
    static <V> DeadLetterTopicWriter<V> create(KafkaClientFactory clientFactory,
                                               Map<String, Object> producerProperties,
                                               Serializer<V> valueSerializer,
                                               String suffix) {
        Map<String, Object> properties = new HashMap<>(producerProperties);
        properties.putIfAbsent(ProducerConfig.MAX_BLOCK_MS_CONFIG,
                               (int) ShutdownBudget.DEAD_LETTER_MAX_BLOCK.toMillis());
        KafkaClientFactory deadLetterClients = clientFactory.forClient(CLIENT_NAME);
        return new DeadLetterTopicWriter<>(
                () -> deadLetterClients.createProducer(properties, new StringSerializer(), valueSerializer),
                suffix
        );
    }

    /**
     * Writes the given {@code record}, which failed with the given {@code failure}, to its dead-letter topic, and waits
     * for the acknowledgement. The producer's {@code max.block.ms} bounds the send, and the wait for the
     * acknowledgement lasts at most {@link ShutdownBudget#DEAD_LETTER_ACK_TIMEOUT}, or until the deadline of the given
     * {@code stop}, once it is requested.
     *
     * @param record  the record that failed
     * @param failure the failure of the record's event
     * @param stop    the stop signal of the consumer writing the record
     * @return {@code true} if the dead-letter record was acknowledged, {@code false} if writing it failed
     */
    boolean write(ConsumerRecord<String, ?> record, Throwable failure, StopSignal stop) {
        String topic = record.topic() + suffix;
        try {
            ProducerRecord<String, V> deadLetter = deadLetterRecord(topic, record, failure);
            CompletableFuture<Void> sent = new CompletableFuture<>();
            producer().send(deadLetter, (metadata, error) -> {
                if (error != null) {
                    sent.completeExceptionally(error);
                } else {
                    sent.complete(FutureUtils.ignoreResult(metadata));
                }
            });
            if (!stop.await(sent, ShutdownBudget.DEAD_LETTER_ACK_TIMEOUT)) {
                throw new TimeoutException("The dead-letter record was not acknowledged in time.");
            }
            // Completed by now; rethrows the failure of the send, if any.
            sent.getNow(null);
            logger.warn("Moved the record at offset [{}] of partition [{}-{}] to dead-letter topic [{}]: processing "
                                + "its event failed.", record.offset(), record.topic(), record.partition(), topic,
                        failure);
            return true;
        } catch (Exception e) {
            logger.warn("Moving the record at offset [{}] of partition [{}-{}] to dead-letter topic [{}] failed. "
                                + "The record is retried instead.",
                        record.offset(), record.topic(), record.partition(), topic, e);
            return false;
        }
    }

    private synchronized Producer<String, V> producer() {
        if (closed) {
            throw new IllegalStateException("The Kafka event source is stopping; no dead-letter records are written "
                                                    + "anymore.");
        }
        if (producer == null) {
            producer = producerFactory.get();
        }
        return producer;
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, V> deadLetterRecord(String topic,
                                                       ConsumerRecord<String, ?> record,
                                                       Throwable failure) {
        RecordHeaders headers = new RecordHeaders(record.headers().toArray());
        headers.add(DeadLetterHeaders.EXCEPTION_CLASS, bytes(failure.getClass().getName()));
        if (failure.getMessage() != null) {
            headers.add(DeadLetterHeaders.EXCEPTION_MESSAGE, bytes(failure.getMessage()));
        }
        headers.add(DeadLetterHeaders.ORIGINAL_TOPIC, bytes(record.topic()));
        headers.add(DeadLetterHeaders.ORIGINAL_PARTITION, bytes(String.valueOf(record.partition())));
        headers.add(DeadLetterHeaders.ORIGINAL_OFFSET, bytes(String.valueOf(record.offset())));
        // The dead-letter record keeps the time of the original event, when the original record has one.
        Long timestamp = record.timestamp() == ConsumerRecord.NO_TIMESTAMP ? null : record.timestamp();
        return new ProducerRecord<>(topic, null, timestamp, record.key(), (V) record.value(), headers);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Closes the producer, if it was created, waiting at most the given {@code timeout} for the records it has not
     * delivered yet. A record whose acknowledgement its consumer did not receive was never committed past, so dropping
     * it leaves the record for the partition's next owner. No records are written after this call.
     *
     * @param timeout the longest time to wait for undelivered records
     */
    synchronized void close(Duration timeout) {
        closed = true;
        if (producer != null) {
            try {
                producer.close(timeout);
            } catch (Exception e) {
                logger.debug("Closing the dead-letter producer failed.", e);
            }
            producer = null;
        }
    }
}
