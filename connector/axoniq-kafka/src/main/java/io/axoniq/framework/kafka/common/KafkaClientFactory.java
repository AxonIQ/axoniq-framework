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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Creates the Kafka clients that the Kafka connector's publishing and consuming components use.
 * <p>
 * A factory holds the client configuration that all clients of an application share, such as the bootstrap servers
 * and the security settings. Each component adds the settings specific to its role when it requests a client, such as
 * a consumer group or a transactional identifier. Those settings take precedence over the shared configuration.
 * <p>
 * {@link #create(Map)} returns a factory that creates the clients of the {@code kafka-clients} library. Another
 * implementation can supply clients that are configured differently, or test doubles.
 * <p>
 * Example usage:
 * <pre>{@code
 * KafkaClientFactory clientFactory = KafkaClientFactory.create(Map.of(
 *         CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
 *         CommonClientConfigs.CLIENT_ID_CONFIG, "order-service"
 * ));
 * Producer<String, byte[]> producer = clientFactory.createProducer(
 *         Map.of(ProducerConfig.ACKS_CONFIG, "all"), new StringSerializer(), new ByteArraySerializer()
 * );
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public interface KafkaClientFactory {

    /**
     * Creates a {@code KafkaClientFactory} that creates the clients of the {@code kafka-clients} library. Each client
     * combines the given shared {@code configuration} with the settings of the component that requests it.
     *
     * @param configuration the client configuration that every client of this factory shares, using the property
     *                      names of the {@code kafka-clients} library
     * @return a factory that creates clients from the given {@code configuration}
     * @throws IllegalArgumentException if the {@code configuration} sets {@code transactional.id} or
     *                                  {@code group.instance.id}
     */
    static KafkaClientFactory create(Map<String, Object> configuration) {
        return new DefaultKafkaClientFactory(configuration);
    }

    /**
     * Returns a factory for the clients of the component with the given {@code componentName}, such as a sink or a
     * source, so that the clients of different components can be told apart.
     * <p>
     * The factory that {@link #create(Map)} returns gives every client of the component a client identifier of its
     * own, when the combined settings contain a {@code client.id}: {@code <client.id>-<componentName>-<n>}, where
     * {@code n} counts the clients the returned factory created, from 0. The
     * {@link io.axoniq.framework.kafka.configuration.KafkaConnector} calls this for each sink and source, with its
     * name. Use {@link #forClient(String)} on the returned factory for a client that should keep its identifier when it
     * is created anew.
     * <p>
     * By default, this returns this factory itself, leaving the settings as they are.
     *
     * @param componentName the name of the component that requests clients
     * @return a factory for the clients of the component
     */
    default KafkaClientFactory forComponent(String componentName) {
        return this;
    }

    /**
     * Returns a factory for one long-lived client of this factory's component, such as one of the consumers of a
     * source, so that the client keeps the same client identifier each time it is created anew, for example after a
     * failure.
     * <p>
     * The factory that {@link #create(Map)} returns gives the clients of the returned factory the client identifier
     * {@code <client.id>-<componentName>-<clientName>}, when the combined settings contain a {@code client.id}, rather
     * than numbering them as {@link #forComponent(String)} does; without a component name, it is
     * {@code <client.id>-<clientName>}. Calling this method on the returned factory replaces the client name. A
     * {@link io.axoniq.framework.kafka.subscribing.KafkaSubscribableEventSource} asks its factory for one per
     * consumer, named after the consumer's index, and for one for its dead-letter producer, named {@code dlt}.
     * <p>
     * Clients that exist at the same time must not share a client name.
     * <p>
     * By default, this returns this factory itself, leaving the settings as they are.
     *
     * @param clientName the name of the client within its component, such as the index of a consumer
     * @return a factory for the client with the given name
     */
    default KafkaClientFactory forClient(String clientName) {
        return this;
    }

    /**
     * Creates a {@link Producer} from the shared configuration and the given {@code properties}. The
     * {@code properties} take precedence over the shared configuration.
     *
     * @param properties      the producer settings specific to the requesting component
     * @param keySerializer   the serializer for record keys
     * @param valueSerializer the serializer for record values
     * @param <K>             the type of record key
     * @param <V>             the type of record value
     * @return a new producer, which the caller owns and must close
     */
    <K, V> Producer<K, V> createProducer(Map<String, Object> properties,
                                         Serializer<K> keySerializer,
                                         Serializer<V> valueSerializer);

    /**
     * Creates a {@link Consumer} from the shared configuration and the given {@code properties}. The
     * {@code properties} take precedence over the shared configuration.
     *
     * @param properties        the consumer settings specific to the requesting component
     * @param keyDeserializer   the deserializer for record keys
     * @param valueDeserializer the deserializer for record values
     * @param <K>               the type of record key
     * @param <V>               the type of record value
     * @return a new consumer, which the caller owns and must close
     */
    <K, V> Consumer<K, V> createConsumer(Map<String, Object> properties,
                                         Deserializer<K> keyDeserializer,
                                         Deserializer<V> valueDeserializer);

    /**
     * Creates an {@link Admin} client from the shared configuration and the given {@code properties}. The
     * {@code properties} take precedence over the shared configuration.
     *
     * @param properties the admin client settings specific to the requesting component
     * @return a new admin client, which the caller owns and must close
     */
    Admin createAdmin(Map<String, Object> properties);
}
