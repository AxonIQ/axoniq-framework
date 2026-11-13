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

package io.axoniq.framework.kafka.utils;

import io.axoniq.framework.kafka.common.KafkaClientFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.axonframework.common.FutureUtils;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * A {@link KafkaClientFactory} handing out Kafka's own {@link MockProducer MockProducers} and
 * {@link MockConsumer MockConsumers}, recording each producer and the properties every client was requested with.
 * Client-scoped factories record the client names they were requested with. Producer and consumer creation can be
 * made to fail, new producers can be prepared before they are handed out, and closing producers can be held up until
 * a gate opens.
 *
 * @author Allard Buijze
 */
public class RecordingKafkaClientFactory implements KafkaClientFactory {

    private final boolean autoComplete;
    private final List<MockProducer<?, ?>> producers = new CopyOnWriteArrayList<>();
    private final List<Map<String, Object>> producerProperties = new CopyOnWriteArrayList<>();
    private final List<Map<String, Object>> consumerProperties = new CopyOnWriteArrayList<>();
    private final List<MockConsumer<?, ?>> consumers = new CopyOnWriteArrayList<>();
    private volatile java.util.function.Consumer<MockConsumer<?, ?>> consumerPreparation = consumer -> {
    };
    private final List<Map<String, Object>> adminProperties = new CopyOnWriteArrayList<>();
    private final List<String> componentNames = new CopyOnWriteArrayList<>();
    private final List<String> consumerClientNames = new CopyOnWriteArrayList<>();
    private final List<String> producerClientNames = new CopyOnWriteArrayList<>();
    private volatile @Nullable RuntimeException creationFailure;
    private volatile @Nullable RuntimeException consumerCreationFailure;
    private volatile java.util.function.Consumer<MockProducer<?, ?>> preparation = producer -> {
    };
    private volatile CompletableFuture<Void> closeGate = FutureUtils.emptyCompletedFuture();

    /**
     * @param autoComplete whether sends complete immediately, or wait for {@link MockProducer#completeNext()}
     */
    public RecordingKafkaClientFactory(boolean autoComplete) {
        this.autoComplete = autoComplete;
    }

    /**
     * Records the given {@code componentName}, and returns this factory itself.
     */
    @Override
    public KafkaClientFactory forComponent(String componentName) {
        componentNames.add(componentName);
        return this;
    }

    public List<String> componentNames() {
        return List.copyOf(componentNames);
    }

    /**
     * Returns a factory that records the given {@code clientName} for every consumer and producer it creates, and
     * creates them through this factory.
     */
    @Override
    public KafkaClientFactory forClient(String clientName) {
        RecordingKafkaClientFactory recording = this;
        return new KafkaClientFactory() {
            @Override
            public <K, V> Producer<K, V> createProducer(Map<String, Object> properties,
                                                        Serializer<K> keySerializer,
                                                        Serializer<V> valueSerializer) {
                producerClientNames.add(clientName);
                return recording.createProducer(properties, keySerializer, valueSerializer);
            }

            @Override
            public <K, V> Consumer<K, V> createConsumer(Map<String, Object> properties,
                                                        Deserializer<K> keyDeserializer,
                                                        Deserializer<V> valueDeserializer) {
                consumerClientNames.add(clientName);
                return recording.createConsumer(properties, keyDeserializer, valueDeserializer);
            }

            @Override
            public Admin createAdmin(Map<String, Object> properties) {
                return recording.createAdmin(properties);
            }
        };
    }

    /**
     * Returns the client names of the consumers requested through a {@link #forClient(String) client-scoped} factory,
     * in the order they were requested.
     */
    public List<String> consumerClientNames() {
        return List.copyOf(consumerClientNames);
    }

    /**
     * Returns the client names of the producers requested through a {@link #forClient(String) client-scoped} factory,
     * in the order they were requested.
     */
    public List<String> producerClientNames() {
        return List.copyOf(producerClientNames);
    }

    @Override
    public <K, V> Producer<K, V> createProducer(Map<String, Object> properties,
                                                Serializer<K> keySerializer,
                                                Serializer<V> valueSerializer) {
        RuntimeException failure = creationFailure;
        if (failure != null) {
            throw failure;
        }
        CompletableFuture<Void> gate = closeGate;
        MockProducer<K, V> producer = new MockProducer<>(autoComplete, null, keySerializer, valueSerializer) {
            @Override
            public void close(Duration timeout) {
                gate.orTimeout(10, TimeUnit.SECONDS).join();
                super.close(timeout);
            }
        };
        preparation.accept(producer);
        producers.add(producer);
        producerProperties.add(Map.copyOf(properties));
        return producer;
    }

    /**
     * Makes creating a producer fail with the given {@code failure}, or succeed again when {@code null}.
     */
    public void failCreationWith(@Nullable RuntimeException failure) {
        this.creationFailure = failure;
    }

    /**
     * Prepares every producer created from now on with the given {@code preparation}, before it is handed out, for
     * example to make its {@code initTransactions} fail.
     */
    public void prepareProducers(java.util.function.Consumer<MockProducer<?, ?>> preparation) {
        this.preparation = preparation;
    }

    /**
     * Holds up closing the producers created from now on until the given {@code gate} completes.
     */
    public void holdUpClosingUntil(CompletableFuture<Void> gate) {
        this.closeGate = gate;
    }

    /**
     * Records the given {@code properties} and hands out a {@link MockConsumer} that knows no topics, or fails when
     * told to with {@link #failConsumerCreationWith(RuntimeException)}.
     */
    @Override
    public <K, V> Consumer<K, V> createConsumer(Map<String, Object> properties,
                                                Deserializer<K> keyDeserializer,
                                                Deserializer<V> valueDeserializer) {
        consumerProperties.add(Map.copyOf(properties));
        RuntimeException failure = consumerCreationFailure;
        if (failure != null) {
            throw failure;
        }
        MockConsumer<K, V> consumer = mockConsumer();
        consumerPreparation.accept(consumer);
        consumers.add(consumer);
        return consumer;
    }

    // The constructor taking a String, which kafka-clients 4 prefers, does not exist in the 3.x line.
    @SuppressWarnings("deprecation")
    private static <K, V> MockConsumer<K, V> mockConsumer() {
        return new MockConsumer<>(OffsetResetStrategy.NONE);
    }

    /**
     * Prepares every consumer created from now on with the given {@code preparation}, before it is handed out, for
     * example to schedule the records it delivers.
     */
    public void prepareConsumers(java.util.function.Consumer<MockConsumer<?, ?>> preparation) {
        this.consumerPreparation = preparation;
    }

    @SuppressWarnings("unchecked")
    public <V> List<MockConsumer<String, V>> consumers() {
        return (List<MockConsumer<String, V>>) (List<?>) List.copyOf(consumers);
    }

    /**
     * Makes creating a consumer fail with the given {@code failure}, after recording its properties, or succeed again
     * when {@code null}.
     */
    public void failConsumerCreationWith(@Nullable RuntimeException failure) {
        this.consumerCreationFailure = failure;
    }

    /**
     * Records the given {@code properties}, and then fails: this factory provides no admin clients.
     */
    @Override
    public Admin createAdmin(Map<String, Object> properties) {
        adminProperties.add(Map.copyOf(properties));
        throw new UnsupportedOperationException("Not supported by this test factory");
    }

    public List<Map<String, Object>> consumerProperties() {
        return List.copyOf(consumerProperties);
    }

    public List<Map<String, Object>> adminProperties() {
        return List.copyOf(adminProperties);
    }

    @SuppressWarnings("unchecked")
    public <V> List<MockProducer<String, V>> producers() {
        return (List<MockProducer<String, V>>) (List<?>) List.copyOf(producers);
    }

    public List<Map<String, Object>> producerProperties() {
        return List.copyOf(producerProperties);
    }
}
