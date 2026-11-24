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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * One Kafka broker, in a container, shared by all integration tests of a test run. Each test isolates itself by using
 * {@link #uniqueTopic(String) topics of its own}.
 *
 * @author Allard Buijze
 */
public final class KafkaTestBroker {

    private static final KafkaContainer CONTAINER =
            new KafkaContainer("apache/kafka-native:4.3.1")
                    // Writing to a topic that does not exist must fail, so tests can provoke delivery failures.
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                    // A single broker cannot satisfy the default replication of the transaction state log.
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");

    private KafkaTestBroker() {
    }

    /**
     * Returns the bootstrap servers of the shared broker, starting it on first use.
     */
    public static synchronized String bootstrapServers() {
        if (!CONTAINER.isRunning()) {
            CONTAINER.start();
        }
        return CONTAINER.getBootstrapServers();
    }

    /**
     * Returns a client factory for the shared broker.
     */
    public static KafkaClientFactory clientFactory() {
        return KafkaClientFactory.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()));
    }

    /**
     * Returns a topic name no other test uses.
     */
    public static String uniqueTopic(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    /**
     * An operation on the shared broker through an {@link Admin} client.
     */
    @FunctionalInterface
    public interface AdminOperation<T> {

        /**
         * Performs the operation with the given {@code admin} client.
         */
        T perform(Admin admin) throws Exception;
    }

    /**
     * Performs the given {@code operation} with an {@link Admin} client of the shared broker, which is closed
     * afterward. Failures are rethrown as {@link IllegalStateException IllegalStateExceptions}.
     */
    public static <T> T withAdmin(String description, AdminOperation<T> operation) {
        try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()))) {
            return operation.perform(admin);
        } catch (Exception e) {
            throw new IllegalStateException(description + " failed", e);
        }
    }

    /**
     * Creates the given {@code topic} with the given number of {@code partitions}, waiting until every partition has a
     * leader.
     */
    public static String createTopic(String topic, int partitions) {
        return withAdmin("Creating topic " + topic, admin -> {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get(30, TimeUnit.SECONDS);
            long deadline = System.currentTimeMillis() + 30_000;
            while (!allPartitionsHaveALeader(admin, topic)) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("Not every partition of topic " + topic + " has a leader.");
                }
                Thread.sleep(50);
            }
            return topic;
        });
    }

    private static boolean allPartitionsHaveALeader(Admin admin, String topic) throws Exception {
        TopicDescription description;
        try {
            description = admin.describeTopics(List.of(topic)).allTopicNames().get(30, TimeUnit.SECONDS).get(topic);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                // The broker's metadata does not show the new topic yet.
                return false;
            }
            throw e;
        }
        return description.partitions().stream().allMatch(partition -> partition.leader() != null);
    }

    /**
     * Adds partitions to the given {@code topic}, so it has {@code totalPartitions} in total.
     */
    public static void increasePartitions(String topic, int totalPartitions) {
        withAdmin("Increasing partitions of " + topic, admin -> admin.createPartitions(
                Map.of(topic, NewPartitions.increaseTo(totalPartitions))
        ).all().get(30, TimeUnit.SECONDS));
    }

    /**
     * Deletes the records of the given {@code partition} before the given {@code offset}, as retention would.
     */
    public static void deleteRecordsBefore(TopicPartition partition, long offset) {
        withAdmin("Deleting records of " + partition, admin -> admin.deleteRecords(
                Map.of(partition, RecordsToDelete.beforeOffset(offset))
        ).all().get(30, TimeUnit.SECONDS));
    }

    /**
     * Returns the offsets the given consumer {@code group} committed.
     */
    public static Map<TopicPartition, OffsetAndMetadata> committedOffsets(String group) {
        return withAdmin("Reading the offsets of group " + group,
                         admin -> admin.listConsumerGroupOffsets(group)
                                       .partitionsToOffsetAndMetadata()
                                       .get(30, TimeUnit.SECONDS));
    }

    /**
     * Returns the number of members of the given consumer {@code group} that have partitions assigned.
     */
    public static long membersWithPartitions(String group) {
        return withAdmin("Describing group " + group,
                         admin -> admin.describeConsumerGroups(List.of(group)).describedGroups().get(group)
                                       .get(30, TimeUnit.SECONDS)
                                       .members().stream()
                                       .filter(member -> !member.assignment().topicPartitions().isEmpty())
                                       .count());
    }

    /**
     * Writes the given {@code records} directly, bypassing the code under test, and waits until the broker has
     * acknowledged each of them. A record the broker rejects, for example because its topic does not exist, fails
     * the call.
     */
    public static void produce(List<ProducerRecord<String, byte[]>> records) {
        // Below the 30 seconds waited for each acknowledgement, so that a stall fails with the producer's own error.
        Map<String, Object> properties = Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                                                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000,
                                                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 25_000);
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(
                properties, new StringSerializer(), new ByteArraySerializer())) {
            List<Future<RecordMetadata>> sent = new ArrayList<>();
            for (ProducerRecord<String, byte[]> record : records) {
                sent.add(producer.send(record));
            }
            for (Future<RecordMetadata> acknowledgement : sent) {
                acknowledgement.get(30, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Producing records failed", e);
        }
    }

    /**
     * Reads all records of the given {@code topic} from the beginning, for the given {@code duration}, with the given
     * isolation level ({@code read_committed} or {@code read_uncommitted}).
     */
    public static List<ConsumerRecord<String, byte[]>> readAll(String topic, String isolationLevel, Duration duration) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer())) {
            int partitions = consumer.partitionsFor(topic).size();
            List<TopicPartition> assignment = IntStream.range(0, partitions)
                                                       .mapToObj(p -> new TopicPartition(topic, p)).toList();
            consumer.assign(assignment);
            consumer.seekToBeginning(assignment);
            List<ConsumerRecord<String, byte[]>> result = new ArrayList<>();
            long deadline = System.currentTimeMillis() + duration.toMillis();
            while (System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(100)).forEach(result::add);
            }
            return result;
        }
    }
}
