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

import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import io.axoniq.framework.kafka.common.KafkaAxoniqAddon;
import io.axoniq.framework.kafka.common.KafkaClientFactory;
import io.axoniq.license.entitlement.EntitlementManager;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An {@link EventSink} that publishes events to Kafka topics.
 * <p>
 * Events are published to the sink explicitly, typically from an event handler. Obtain the sink by name, with
 * {@code configuration.getComponent(EventSink.class, "outbound")}, or in Spring by injecting an {@code EventSink}
 * qualified with {@code @Qualifier("outbound")}:
 * <pre>{@code
 * @EventHandler
 * CompletableFuture<Void> on(OrderShipped event, MessageTypeResolver typeResolver, ProcessingContext context) {
 *     if (!event.international()) {
 *         return FutureUtils.emptyCompletedFuture();
 *     }
 *     return EventSinks.publishPayloads(kafka, context, typeResolver,
 *                                       new ParcelDispatched(event.parcelId(), event.destinationCountry()));
 * }
 * }</pre>
 * See {@link EventSinks} for publishing payloads.
 * <p>
 * For every event, the sink resolves the topic (skipping the event when there is none) and the record key, and
 * encodes the event with the configured {@link KafkaEventCodec}. The {@link ConfirmationMode} decides how publishing
 * relates to the given {@link ProcessingContext}. Outside a processing context, the sink sends right away, and the
 * returned future completes when the chosen mode has confirmed the events. Within a processing context, the returned
 * future completes once the records are handed to Kafka, and the confirmation is part of committing the context. The
 * producer's {@code max.block.ms} setting bounds how long sending may block.
 * <p>
 * In the {@link ConfirmationMode#WAIT_FOR_ACK} and {@link ConfirmationMode#TRANSACTIONAL} modes, publishing in a
 * context that has started to commit is rejected with an {@link IllegalStateException} before anything is sent, and
 * failures to publish are reported as {@link KafkaPublicationException KafkaPublicationExceptions}. The
 * {@link ConfirmationMode#NONE} mode ignores the context, so it does not reject publishing in a committing context, and
 * it only logs failed sends. In every mode, publishing after {@link #shutdown()} is rejected with an
 * {@link IllegalStateException}.
 * <p>
 * From a pooled streaming processor's handler, {@code WAIT_FOR_ACK} and {@code TRANSACTIONAL} deliver events at least
 * once only when the processor's token store takes part in the processing context's transaction, as the JPA and JDBC
 * token stores do with a transaction manager. Otherwise, a failed confirmation can lose that context's events.
 * <p>
 * The {@link io.axoniq.framework.kafka.configuration.KafkaConnector} registers each sink as an {@link EventSink}
 * component under the sink's name, separate from the application's own event sink. The registered component may be
 * decorated: depend on {@link EventSink}, not on {@code KafkaEventSink}. Call {@link #shutdown()} to close the sink's
 * producers when the application stops.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public class KafkaEventSink implements EventSink {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEventSink.class);
    /**
     * The longest time closing the producers takes, below the 30 seconds that Spring Boot gives a lifecycle phase by
     * default.
     */
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(29);

    private final KafkaEventSinkConfiguration configuration;
    private final Publisher<?> publisher;
    private final EntitlementManager entitlementManager;

    /**
     * Constructs a sink that publishes events as the given {@code configuration} describes, with producers created by
     * the given {@code clientFactory}.
     *
     * @param clientFactory the factory creating the producers of this sink
     * @param configuration the settings of this sink
     * @throws IllegalArgumentException if the configuration uses the {@link ConfirmationMode#TRANSACTIONAL} mode
     *                                  without a transactional identifier prefix
     */
    public KafkaEventSink(KafkaClientFactory clientFactory, KafkaEventSinkConfiguration configuration) {
        this(clientFactory, configuration, EntitlementManager.INSTANCE);
        EntitlementManager.INSTANCE.registerAddon(KafkaAxoniqAddon.class);
    }

    /**
     * Constructs a sink that claims published events against the given {@code entitlementManager}.
     *
     * @param clientFactory      the factory creating the producers of this sink
     * @param configuration      the settings of this sink
     * @param entitlementManager the entitlement manager published events are claimed against
     */
    KafkaEventSink(KafkaClientFactory clientFactory,
                   KafkaEventSinkConfiguration configuration,
                   EntitlementManager entitlementManager) {
        Objects.requireNonNull(clientFactory, "The clientFactory must not be null.");
        this.configuration = Objects.requireNonNull(configuration, "The configuration must not be null.");
        this.entitlementManager = Objects.requireNonNull(entitlementManager,
                                                         "The entitlementManager must not be null.");
        if (configuration.confirmationMode() == ConfirmationMode.TRANSACTIONAL
                && configuration.transactionalIdPrefix() == null) {
            throw new IllegalArgumentException(
                    "The TRANSACTIONAL confirmation mode requires a transactional identifier prefix."
            );
        }
        this.publisher = new Publisher<>(clientFactory, configuration, configuration.codec());
    }

    @Override
    public CompletableFuture<Void> publish(@Nullable ProcessingContext context,
                                           List<? extends EventMessage> events) {
        Objects.requireNonNull(events, "The events must not be null.");
        try {
            return publisher.publish(context, events);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Closes the producers of this sink. Events published afterward are rejected right away with an
     * {@link IllegalStateException}; the producers are closed asynchronously and in parallel, given at most 29
     * seconds together to send what they have buffered. A producer that is in use while the sink shuts down is
     * closed once the processing context that uses it ends.
     *
     * @return a future that completes when the producers are closed
     */
    public CompletableFuture<Void> shutdown() {
        publisher.rejectPublications();
        return CompletableFuture.runAsync(publisher::close, Thread.ofVirtual().name("KafkaEventSink-shutdown")::start);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("codec", configuration.codec());
        descriptor.describeProperty("topicResolver", configuration.topicResolver());
        descriptor.describeProperty("recordKeyResolver", configuration.recordKeyResolver());
        descriptor.describeProperty("confirmationMode", configuration.confirmationMode());
        descriptor.describeProperty("ackTimeout", configuration.ackTimeout());
    }

    /**
     * Does the actual publishing for one record value type, so that the sink itself needs no type parameter.
     */
    private class Publisher<V> {

        private final KafkaClientFactory clientFactory;
        private final KafkaEventCodec<V> codec;
        // Present in the TRANSACTIONAL confirmation mode only.
        private final @Nullable TransactionalPublisher<V> transactional;
        private final ResourceKey<PendingAcks> pendingAcksKey = ResourceKey.withLabel("kafkaEventSinkPendingAcks");
        private volatile @Nullable Producer<String, V> sharedProducer;
        private volatile boolean closed;

        private Publisher(KafkaClientFactory clientFactory,
                          KafkaEventSinkConfiguration configuration,
                          KafkaEventCodec<V> codec) {
            this.clientFactory = clientFactory;
            this.codec = codec;
            String prefix = configuration.transactionalIdPrefix();
            this.transactional = configuration.confirmationMode() == ConfirmationMode.TRANSACTIONAL && prefix != null
                    ? new TransactionalPublisher<>(clientFactory,
                                                   configuration.producerProperties(),
                                                   codec.valueSerde().serializer(),
                                                   prefix,
                                                   configuration.commitExecutor(),
                                                   entitlementManager)
                    : null;
        }

        private CompletableFuture<Void> publish(@Nullable ProcessingContext context,
                                                List<? extends EventMessage> events) {
            List<ProducerRecord<String, V>> records = encode(context, events);
            if (records.isEmpty()) {
                return FutureUtils.emptyCompletedFuture();
            }
            return switch (configuration.confirmationMode()) {
                case NONE -> publishWithoutConfirmation(records);
                case WAIT_FOR_ACK -> publishAwaitingAcks(context, records);
                case TRANSACTIONAL -> publishTransactionally(context, records);
            };
        }

        private List<ProducerRecord<String, V>> encode(@Nullable ProcessingContext context,
                                                       List<? extends EventMessage> events) {
            List<ProducerRecord<String, V>> records = new ArrayList<>(events.size());
            for (EventMessage event : events) {
                Optional<String> topic = configuration.topicResolver().resolve(event, context);
                if (topic.isPresent()) {
                    String key = configuration.recordKeyResolver().resolve(event, context);
                    records.add(codec.encode(event, topic.get(), key, context));
                }
            }
            return records;
        }

        private void claim(int count) {
            PublishingSupport.claim(entitlementManager, count);
        }

        private CompletableFuture<Void> publishWithoutConfirmation(List<ProducerRecord<String, V>> records) {
            Producer<String, V> producer = sharedProducer();
            for (ProducerRecord<String, V> record : records) {
                PublishingSupport.send(producer, record).whenComplete((result, error) -> {
                    if (error != null) {
                        logger.warn("Publishing an event to Kafka topic [{}] failed.", record.topic(), error);
                    }
                });
            }
            claim(records.size());
            return FutureUtils.emptyCompletedFuture();
        }

        private CompletableFuture<Void> publishAwaitingAcks(@Nullable ProcessingContext context,
                                                            List<ProducerRecord<String, V>> records) {
            if (context == null) {
                Producer<String, V> producer = sharedProducer();
                List<CompletableFuture<Void>> acks = records.stream()
                                                            .map(record -> PublishingSupport.send(producer, record))
                                                            .toList();
                return awaitAll(acks, records.size());
            }
            PendingAcks pending = context.computeResourceIfAbsent(pendingAcksKey, () -> {
                PendingAcks created = new PendingAcks();
                // Registered before anything is sent, so that a publish too late in the context sends nothing.
                PublishingSupport.registerLate(() -> context.onPrepareCommit(c -> created.await()));
                return created;
            });
            pending.sendAll(records);
            return FutureUtils.emptyCompletedFuture();
        }

        private CompletableFuture<Void> awaitAll(List<CompletableFuture<Void>> acks, int count) {
            long timeout = configuration.ackTimeout().toMillis();
            return CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new))
                                    .orTimeout(timeout, TimeUnit.MILLISECONDS)
                                    .handle((result, error) -> {
                                        if (error == null) {
                                            claim(count);
                                            return FutureUtils.ignoreResult(result);
                                        }
                                        Throwable cause = FutureUtils.unwrap(error);
                                        if (cause instanceof KafkaPublicationException publicationException) {
                                            throw publicationException;
                                        }
                                        if (cause instanceof TimeoutException) {
                                            throw new KafkaPublicationException(
                                                    "Kafka did not acknowledge the published events within "
                                                            + configuration.ackTimeout() + ".", cause
                                            );
                                        }
                                        throw new KafkaPublicationException("Publishing events to Kafka failed.",
                                                                            cause);
                                    });
        }

        private CompletableFuture<Void> publishTransactionally(@Nullable ProcessingContext context,
                                                               List<ProducerRecord<String, V>> records) {
            // The constructor rejects the TRANSACTIONAL mode without a transactional identifier prefix.
            TransactionalPublisher<V> transactionalPublisher = Objects.requireNonNull(transactional);
            if (context == null) {
                return transactionalPublisher.publishOutsideContext(records);
            }
            transactionalPublisher.send(context, records);
            return FutureUtils.emptyCompletedFuture();
        }

        private Producer<String, V> sharedProducer() {
            if (closed) {
                throw PublishingSupport.sinkShutDown();
            }
            Producer<String, V> producer = sharedProducer;
            if (producer == null) {
                synchronized (this) {
                    if (closed) {
                        throw PublishingSupport.sinkShutDown();
                    }
                    producer = sharedProducer;
                    if (producer == null) {
                        try {
                            producer = clientFactory.createProducer(configuration.producerProperties(),
                                                                    new StringSerializer(),
                                                                    codec.valueSerde().serializer());
                        } catch (RuntimeException e) {
                            throw new KafkaPublicationException("Creating a Kafka producer failed.", e);
                        }
                        sharedProducer = producer;
                    }
                }
            }
            return producer;
        }

        /**
         * Rejects publications from now on, before the producers are closed.
         */
        private void rejectPublications() {
            closed = true;
            if (transactional != null) {
                transactional.rejectTransactions();
            }
        }

        private synchronized void close() {
            closed = true;
            Producer<String, V> producer = sharedProducer;
            sharedProducer = null;
            if (producer != null) {
                producer.close(CLOSE_TIMEOUT);
            }
            if (transactional != null) {
                transactional.close(CLOSE_TIMEOUT);
            }
        }

        /**
         * The acknowledgements a processing context waits for before it commits. Once the context starts waiting,
         * further sends are rejected rather than left unconfirmed.
         */
        private class PendingAcks {

            private final List<CompletableFuture<Void>> acks = new ArrayList<>();
            private int count;
            private boolean awaiting;

            private synchronized void sendAll(List<ProducerRecord<String, V>> records) {
                if (awaiting) {
                    throw new IllegalStateException(
                            "Events can no longer be published to Kafka in this processing context: it is already "
                                    + "waiting for Kafka to acknowledge the events it published."
                    );
                }
                Producer<String, V> producer = sharedProducer();
                for (ProducerRecord<String, V> record : records) {
                    acks.add(PublishingSupport.send(producer, record));
                }
                count += records.size();
            }

            private CompletableFuture<Void> await() {
                List<CompletableFuture<Void>> snapshot;
                int total;
                synchronized (this) {
                    awaiting = true;
                    snapshot = List.copyOf(acks);
                    total = count;
                }
                return awaitAll(snapshot, total);
            }
        }
    }
}
