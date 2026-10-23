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

package io.axoniq.framework.kafka.streaming;

import io.axoniq.framework.kafka.common.ConsumerSettings;
import io.axoniq.framework.kafka.common.KafkaClientFactory;
import io.axoniq.framework.kafka.common.TopicSubscription;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Looks up the partitions and offsets of the topics a {@link KafkaStreamableEventSource} subscribes to, with a metadata
 * client of its own.
 * <p>
 * Named topics are described directly, so that the source does not fetch the metadata of every topic in the cluster;
 * only a topic pattern requires listing all topics. Describing topics never creates them, and a named topic that does
 * not exist has no partitions, until a later lookup finds it. Offsets are listed with the isolation level of the
 * source's consumers, so that tokens match what the streams read.
 * <p>
 * The metadata client is created when first needed, with the connection settings among the source's consumer
 * properties, and closed when the source shuts down.
 * <p>
 * Internal, because it is created by and only reachable through {@link KafkaStreamableEventSource}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
final class TopicMetadata {

    private static final Duration ADMIN_CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private final KafkaClientFactory clientFactory;
    private final TopicSubscription subscription;
    private final Map<String, Object> consumerProperties;
    private volatile @Nullable Admin admin;
    private boolean closed;

    /**
     * Constructs the metadata lookup of a source with the given {@code subscription} and {@code consumerProperties},
     * whose metadata client the given {@code clientFactory} creates.
     *
     * @param clientFactory      the factory creating the metadata client
     * @param subscription       the topics the source subscribes to
     * @param consumerProperties the consumer properties configured for the source
     */
    TopicMetadata(KafkaClientFactory clientFactory,
                  TopicSubscription subscription,
                  Map<String, Object> consumerProperties) {
        this.clientFactory = clientFactory;
        this.subscription = subscription;
        this.consumerProperties = consumerProperties;
    }

    /**
     * Returns the partitions of the subscribed topics, as they are now, for the polling threads of the streams, which
     * look for new partitions every metadata refresh interval.
     *
     * @param timeout the longest time to wait for the lookup
     * @return the partitions of the subscribed topics that exist
     * @throws KafkaException        if the lookup fails or times out
     * @throws IllegalStateException if the source is shut down
     */
    Set<TopicPartition> partitions(Duration timeout) {
        try {
            return partitions(admin()).orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw new KafkaException("Looking up the partitions of the subscribed Kafka topics failed.", e.getCause());
        }
    }

    /**
     * Returns the offsets that the given {@code spec} selects in every partition of the subscribed topics.
     *
     * @param spec the offsets to look up, such as the earliest or latest ones
     * @return a future of the offset of every partition, which completes exceptionally when the lookup fails
     */
    CompletableFuture<Map<TopicPartition, Long>> offsets(OffsetSpec spec) {
        Admin client;
        try {
            client = admin();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        return partitions(client)
                .thenCompose(partitions -> {
                    if (partitions.isEmpty()) {
                        return CompletableFuture.completedFuture(Map.of());
                    }
                    Map<TopicPartition, OffsetSpec> request =
                            partitions.stream().collect(Collectors.toMap(p -> p, p -> spec));
                    return client.listOffsets(request, listOffsetsOptions())
                                 .all().toCompletionStage().toCompletableFuture();
                })
                .thenApply(result -> result.entrySet().stream()
                                           .collect(Collectors.toMap(Map.Entry::getKey,
                                                                     e -> e.getValue().offset())));
    }

    private CompletableFuture<Set<TopicPartition>> partitions(Admin client) {
        if (subscription instanceof TopicSubscription.Topics(Set<String> names)) {
            return describe(client, names);
        }
        return client.listTopics().names().toCompletionStage().toCompletableFuture()
                     .thenCompose(names -> describe(client, names.stream()
                                                                 .filter(subscription::includes)
                                                                 .collect(Collectors.toSet())));
    }

    private static CompletableFuture<Set<TopicPartition>> describe(Admin client, Set<String> topics) {
        if (topics.isEmpty()) {
            return CompletableFuture.completedFuture(Set.of());
        }
        List<CompletableFuture<List<TopicPartition>>> lookups =
                client.describeTopics(topics).topicNameValues().values().stream()
                      .map(description -> description.toCompletionStage().toCompletableFuture()
                                                     .thenApply(d -> partitionsOf(d).toList())
                                                     .exceptionally(TopicMetadata::noPartitionsIfMissing))
                      .toList();
        return CompletableFuture.allOf(lookups.toArray(CompletableFuture[]::new))
                                .thenApply(done -> lookups.stream()
                                                          .flatMap(lookup -> lookup.getNow(List.of()).stream())
                                                          .collect(Collectors.toSet()));
    }

    private static List<TopicPartition> noPartitionsIfMissing(Throwable failure) {
        Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                ? failure.getCause() : failure;
        if (cause instanceof UnknownTopicOrPartitionException) {
            // A named topic that does not exist (yet) is not read, until a metadata refresh finds it.
            return List.of();
        }
        throw failure instanceof CompletionException completion ? completion : new CompletionException(cause);
    }

    private static Stream<TopicPartition> partitionsOf(TopicDescription description) {
        return description.partitions().stream().map(p -> new TopicPartition(description.name(), p.partition()));
    }

    private Admin admin() {
        Admin client = admin;
        if (client == null) {
            synchronized (this) {
                if (closed) {
                    throw new IllegalStateException("The Kafka event source is shut down.");
                }
                client = admin;
                if (client == null) {
                    client = clientFactory.createAdmin(adminProperties());
                    admin = client;
                }
            }
        }
        return client;
    }

    private ListOffsetsOptions listOffsetsOptions() {
        // Token positions must match what the streams read: with read_committed, up to the last stable offset.
        return ConsumerSettings.readsUncommitted(consumerProperties)
                ? new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED)
                : new ListOffsetsOptions(IsolationLevel.READ_COMMITTED);
    }

    private Map<String, Object> adminProperties() {
        // The connection settings of the source's consumers (servers, security, client identity) apply to its
        // metadata client too.
        Set<String> adminSettings = AdminClientConfig.configNames();
        Map<String, Object> properties = new HashMap<>();
        consumerProperties.forEach((name, value) -> {
            if (adminSettings.contains(name)) {
                properties.put(name, value);
            }
        });
        return properties;
    }

    /**
     * Closes the metadata client, if it was created. Lookups after this call fail.
     */
    void close() {
        Admin client;
        synchronized (this) {
            closed = true;
            client = admin;
            admin = null;
        }
        if (client != null) {
            client.close(ADMIN_CLOSE_TIMEOUT);
        }
    }
}
