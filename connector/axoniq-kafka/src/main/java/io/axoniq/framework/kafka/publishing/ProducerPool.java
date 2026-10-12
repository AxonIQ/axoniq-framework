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

package io.axoniq.framework.kafka.publishing;

import io.axoniq.framework.kafka.common.KafkaClientFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.common.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * A pool of transactional {@link Producer Producers}, so that each processing context publishes in a Kafka transaction
 * of its own.
 * <p>
 * A Kafka producer runs one transaction at a time, while processing contexts run concurrently. The pool hands out an
 * idle producer, or creates a new one when none is idle. A new producer gets the lowest transactional identifier that
 * no live producer of the pool uses: {@code <prefix>0}, {@code <prefix>1}, and so on. The identifiers of discarded
 * producers are reused first.
 * <p>
 * Kafka fences off an older producer when a new one initializes with the same transactional identifier, and aborts
 * the older producer's open transaction. That protects against producers a crashed instance left behind only for the
 * identifiers the restarted instance uses again: a restarted instance that needs fewer producers leaves the open
 * transactions of its higher identifiers to expire after {@code transaction.timeout.ms}, which holds back consumers
 * reading with {@code isolation.level=read_committed} until then. Instances that share a prefix fence each other's
 * producers continually, so every application instance needs a prefix of its own.
 * <p>
 * Internal, because the {@link TransactionalPublisher} of a {@link KafkaEventSink} owns its pool.
 *
 * @param <V> the type of record value the producers send
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
class ProducerPool<V> {

    private static final Logger logger = LoggerFactory.getLogger(ProducerPool.class);
    private static final Duration CLOSE_MARGIN = Duration.ofSeconds(1);

    private final KafkaClientFactory clientFactory;
    private final Map<String, Object> producerProperties;
    private final Serializer<V> valueSerializer;
    private final String transactionalIdPrefix;
    private final Deque<Producer<String, V>> idle = new ArrayDeque<>();
    private final Map<Producer<String, V>, Integer> identifiers = new IdentityHashMap<>();
    private final TreeSet<Integer> freeIdentifiers = new TreeSet<>();
    private int nextIdentifier;
    private Duration closeTimeout = Duration.ofSeconds(30);
    private boolean closed;

    /**
     * Constructs a pool that creates transactional producers through the given {@code clientFactory}.
     *
     * @param clientFactory         the factory creating the producers
     * @param producerProperties    the producer settings specific to the owning sink
     * @param valueSerializer       the serializer for record values
     * @param transactionalIdPrefix the prefix of the transactional identifiers of the producers
     */
    ProducerPool(KafkaClientFactory clientFactory,
                 Map<String, Object> producerProperties,
                 Serializer<V> valueSerializer,
                 String transactionalIdPrefix) {
        this.clientFactory = clientFactory;
        this.producerProperties = producerProperties;
        this.valueSerializer = valueSerializer;
        this.transactionalIdPrefix = transactionalIdPrefix;
    }

    /**
     * Hands out an idle producer, or creates one when none is idle. The caller owns the producer until it
     * {@link #release(Producer) releases} or {@link #discard(Producer) discards} it.
     *
     * @return a transactional producer that is not running a transaction
     * @throws IllegalStateException if the pool is closed
     */
    Producer<String, V> borrow() {
        int identifier;
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("The Kafka sink is shut down and no longer publishes events.");
            }
            Producer<String, V> producer = idle.poll();
            if (producer != null) {
                return producer;
            }
            identifier = freeIdentifiers.isEmpty() ? nextIdentifier++ : freeIdentifiers.pollFirst();
        }
        return create(identifier);
    }

    private Producer<String, V> create(int identifier) {
        Map<String, Object> properties = new HashMap<>(producerProperties);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalIdPrefix + identifier);
        Producer<String, V> producer;
        try {
            // A producer created anew for a discarded one takes over its client identifier, like its transactional one.
            producer = clientFactory.forClient("tx-" + identifier)
                                    .createProducer(properties, new StringSerializer(), valueSerializer);
        } catch (RuntimeException e) {
            freeIdentifier(identifier);
            throw e;
        }
        try {
            producer.initTransactions();
        } catch (RuntimeException e) {
            closeQuietly(producer, Duration.ZERO);
            freeIdentifier(identifier);
            throw e;
        }
        synchronized (this) {
            identifiers.put(producer, identifier);
        }
        return producer;
    }

    private synchronized void freeIdentifier(int identifier) {
        freeIdentifiers.add(identifier);
    }

    /**
     * Returns the given {@code producer} to the pool, to run another transaction. Closes the producer instead when the
     * pool is closed.
     *
     * @param producer the producer to return, which is not running a transaction
     */
    void release(Producer<String, V> producer) {
        synchronized (this) {
            if (!closed) {
                idle.push(producer);
                return;
            }
            identifiers.remove(producer);
        }
        closeQuietly(producer, closeTimeout);
    }

    /**
     * Closes the given {@code producer} rather than returning it to the pool, for example because it can no longer run
     * transactions. Its transactional identifier becomes available to the next producer the pool creates.
     *
     * @param producer the producer to close
     */
    void discard(Producer<String, V> producer) {
        synchronized (this) {
            Integer identifier = identifiers.remove(producer);
            if (identifier != null && !closed) {
                freeIdentifiers.add(identifier);
            }
        }
        closeQuietly(producer, Duration.ZERO);
    }

    /**
     * Closes the pool and its idle producers, in parallel, so that closing a pool of many producers takes no longer
     * than closing one, and at most the given {@code timeout} in all. Producers that are in use are closed when they
     * are released.
     *
     * @param timeout the longest time to wait for the producers to close
     */
    void close(Duration timeout) {
        Deque<Producer<String, V>> toClose;
        synchronized (this) {
            closed = true;
            closeTimeout = timeout;
            toClose = new ArrayDeque<>(idle);
            idle.clear();
            toClose.forEach(identifiers::remove);
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        // Each producer bounds its own close by the timeout, less a margin that covers starting and ending the threads.
        Duration margin = timeout.dividedBy(10).compareTo(CLOSE_MARGIN) < 0 ? timeout.dividedBy(10) : CLOSE_MARGIN;
        Duration producerTimeout = timeout.minus(margin);
        List<Thread> closing = new ArrayList<>();
        for (Producer<String, V> producer : toClose) {
            closing.add(Thread.ofVirtual().name("ProducerPool-close")
                              .start(() -> closeQuietly(producer, producerTimeout)));
        }
        for (Thread thread : closing) {
            try {
                if (!thread.join(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())))) {
                    logger.debug("A Kafka producer did not close within {}.", timeout);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static void closeQuietly(Producer<?, ?> producer, Duration timeout) {
        try {
            producer.close(timeout);
        } catch (RuntimeException e) {
            logger.debug("Closing a Kafka producer failed.", e);
        }
    }
}
