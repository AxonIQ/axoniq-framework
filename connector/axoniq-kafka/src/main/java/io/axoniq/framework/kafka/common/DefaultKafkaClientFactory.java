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

package io.axoniq.framework.kafka.common;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@link KafkaClientFactory} that creates the clients of the {@code kafka-clients} library.
 * <p>
 * Package-private, because applications obtain it through {@link KafkaClientFactory#create(Map)}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
class DefaultKafkaClientFactory implements KafkaClientFactory {

    private final Map<String, Object> configuration;
    private final @Nullable String componentName;
    private final @Nullable String clientName;
    private final AtomicInteger clientCount = new AtomicInteger();

    /**
     * Constructs a factory that creates clients from the given shared {@code configuration}.
     *
     * @param configuration the client configuration that every client of this factory shares
     */
    DefaultKafkaClientFactory(Map<String, Object> configuration) {
        this(configuration, null, null);
    }

    private DefaultKafkaClientFactory(Map<String, Object> configuration,
                                      @Nullable String componentName,
                                      @Nullable String clientName) {
        this.componentName = componentName;
        this.clientName = clientName;
        this.configuration = Map.copyOf(Objects.requireNonNull(configuration, "The configuration must not be null."));
        if (this.configuration.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)) {
            throw new IllegalArgumentException(
                    "The shared client configuration must not set " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + ": "
                            + "producers that share a transactional identifier fence each other off, and producers "
                            + "that are not transactional fail with one. Configure a transactional identifier prefix "
                            + "on the sink instead."
            );
        }
        if (this.configuration.containsKey(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG)) {
            throw new IllegalArgumentException(
                    "The shared client configuration must not set " + ConsumerConfig.GROUP_INSTANCE_ID_CONFIG + ": "
                            + "consumers that share a group instance identifier fence each other off. Set it in the "
                            + "consumer properties of a subscribable source instead."
            );
        }
    }

    @Override
    public KafkaClientFactory forComponent(String componentName) {
        Objects.requireNonNull(componentName, "The componentName must not be null.");
        return new DefaultKafkaClientFactory(configuration, componentName, null);
    }

    @Override
    public KafkaClientFactory forClient(String clientName) {
        Objects.requireNonNull(clientName, "The clientName must not be null.");
        return new DefaultKafkaClientFactory(configuration, componentName, clientName);
    }

    @Override
    public <K, V> Producer<K, V> createProducer(Map<String, Object> properties,
                                                Serializer<K> keySerializer,
                                                Serializer<V> valueSerializer) {
        Objects.requireNonNull(keySerializer, "The keySerializer must not be null.");
        Objects.requireNonNull(valueSerializer, "The valueSerializer must not be null.");
        return new KafkaProducer<>(combinedWith(properties), keySerializer, valueSerializer);
    }

    @Override
    public <K, V> Consumer<K, V> createConsumer(Map<String, Object> properties,
                                                Deserializer<K> keyDeserializer,
                                                Deserializer<V> valueDeserializer) {
        Objects.requireNonNull(keyDeserializer, "The keyDeserializer must not be null.");
        Objects.requireNonNull(valueDeserializer, "The valueDeserializer must not be null.");
        return new KafkaConsumer<>(combinedWith(properties), keyDeserializer, valueDeserializer);
    }

    @Override
    public Admin createAdmin(Map<String, Object> properties) {
        return Admin.create(combinedWith(properties));
    }

    private Map<String, Object> combinedWith(Map<String, Object> properties) {
        Objects.requireNonNull(properties, "The properties must not be null.");
        Map<String, Object> combined = new HashMap<>(configuration);
        combined.putAll(properties);
        Object clientId = combined.get(CommonClientConfigs.CLIENT_ID_CONFIG);
        if (clientId == null || String.valueOf(clientId).isBlank()) {
            return combined;
        }
        String prefix = componentName != null ? clientId + "-" + componentName : String.valueOf(clientId);
        if (clientName != null) {
            combined.put(CommonClientConfigs.CLIENT_ID_CONFIG, prefix + "-" + clientName);
        } else if (componentName != null) {
            combined.put(CommonClientConfigs.CLIENT_ID_CONFIG, prefix + "-" + clientCount.getAndIncrement());
        }
        return combined;
    }
}
