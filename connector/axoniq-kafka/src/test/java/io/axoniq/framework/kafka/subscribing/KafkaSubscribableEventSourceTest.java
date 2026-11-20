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

import io.axoniq.framework.kafka.codec.CodecTestFixtures;
import io.axoniq.framework.kafka.codec.DefaultKafkaEventCodec;
import io.axoniq.framework.kafka.common.KafkaClientFactory;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.framework.kafka.utils.ConsumedRecords;
import io.axoniq.framework.kafka.utils.RecordingEntitlementManager;
import io.axoniq.framework.kafka.utils.RecordingKafkaClientFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.Registration;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class KafkaSubscribableEventSourceTest {

    private final RecordingKafkaClientFactory clientFactory = new RecordingKafkaClientFactory(true);
    private final KafkaSubscribableEventSourceConfiguration configuration =
            KafkaSubscribableEventSourceConfiguration.create(
                    CodecTestFixtures.defaultCodec(),
                    TopicSubscription.topics("parcels"),
                    "billing"
            ).withPollTimeout(Duration.ofMillis(10)).withRetryBackoff(Duration.ofMillis(10), Duration.ofMillis(50));
    private KafkaSubscribableEventSource testSubject = source(configuration);

    private KafkaSubscribableEventSource source(KafkaSubscribableEventSourceConfiguration configuration) {
        return new KafkaSubscribableEventSource(clientFactory,
                                                configuration,
                                                UnitOfWorkTestUtils.SIMPLE_FACTORY,
                                                new RecordingEntitlementManager());
    }

    private void subscribeAndStart() {
        testSubject.subscribe((events, context) -> FutureUtils.emptyCompletedFuture());
        testSubject.start();
    }

    @AfterEach
    void tearDown() {
        testSubject.shutdown().orTimeout(30, TimeUnit.SECONDS).join();
    }

    @Nested
    class Failures {

        @Test
        void reportsAFailureThatRetryingCannotResolve() {
            // given
            clientFactory.failConsumerCreationWith(
                    new KafkaException("Failed to construct kafka consumer", new ConfigException("bad settings"))
            );

            // when
            subscribeAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> testSubject.status().isFailed());
            assertThat(testSubject.status()).isInstanceOfSatisfying(
                    KafkaSourceStatus.Failed.class,
                    failed -> assertThat(failed.cause()).hasRootCauseInstanceOf(ConfigException.class)
            );
            assertThat(clientFactory.consumerProperties()).hasSize(1);
        }

        @Test
        void aFailureThatRetryingCannotResolveStopsAllConsumers() {
            // given - the first of two consumers is not authorized to read the topic, which it finds out once the
            // other consumer was created as well
            AtomicInteger created = new AtomicInteger();
            clientFactory.prepareConsumers(consumer -> {
                if (created.getAndIncrement() == 0) {
                    consumer.schedulePollTask(() -> {
                        await().atMost(Duration.ofSeconds(5)).until(() -> created.get() == 2);
                        consumer.setPollException(new TopicAuthorizationException(Set.of("parcels")));
                    });
                }
            });
            testSubject = source(configuration.withConsumerCount(2));

            // when
            subscribeAndStart();

            // then - the other consumer stops as well, and neither rejoins the group
            await().atMost(Duration.ofSeconds(5)).until(() -> testSubject.status().isFailed());
            await().atMost(Duration.ofSeconds(30)).until(
                    () -> clientFactory.consumers().stream().allMatch(MockConsumer::closed)
            );
            assertThat(clientFactory.consumers()).hasSize(2);
            assertThat(testSubject.status()).isInstanceOf(KafkaSourceStatus.Failed.class);
        }

        @Test
        void aFailureWhileCreatingTheConsumersStartsNone() {
            // given - a client factory that rejects the name of the second consumer
            KafkaClientFactory rejectingSecondClient = new KafkaClientFactory() {
                @Override
                public KafkaClientFactory forClient(String clientName) {
                    if (clientName.equals("1")) {
                        throw new IllegalArgumentException("Client name [1] is not allowed.");
                    }
                    return this;
                }

                @Override
                public <K, V> Producer<K, V> createProducer(Map<String, Object> properties,
                                                            Serializer<K> keySerializer,
                                                            Serializer<V> valueSerializer) {
                    return clientFactory.createProducer(properties, keySerializer, valueSerializer);
                }

                @Override
                public <K, V> Consumer<K, V> createConsumer(Map<String, Object> properties,
                                                            Deserializer<K> keyDeserializer,
                                                            Deserializer<V> valueDeserializer) {
                    return clientFactory.createConsumer(properties, keyDeserializer, valueDeserializer);
                }

                @Override
                public Admin createAdmin(Map<String, Object> properties) {
                    return clientFactory.createAdmin(properties);
                }
            };
            testSubject = new KafkaSubscribableEventSource(rejectingSecondClient,
                                                           configuration.withConsumerCount(2),
                                                           UnitOfWorkTestUtils.SIMPLE_FACTORY,
                                                           new RecordingEntitlementManager());
            testSubject.subscribe((events, context) -> FutureUtils.emptyCompletedFuture());

            // when
            Throwable failure = catchThrowable(testSubject::start);

            // then - all consumers are created before any starts, so none was even created
            assertThat(failure).isInstanceOf(IllegalArgumentException.class);
            assertThat(clientFactory.consumers()).isEmpty();
            assertThat(testSubject.status()).isEqualTo(new KafkaSourceStatus.Stopped());
        }

        @Test
        void subscribingAnewAfterAFailureStartsConsumingAgain() {
            // given
            clientFactory.failConsumerCreationWith(
                    new KafkaException("Failed to construct kafka consumer", new ConfigException("bad settings"))
            );
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> subscriber =
                    (events, context) -> FutureUtils.emptyCompletedFuture();
            Registration registration = testSubject.subscribe(subscriber);
            testSubject.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> testSubject.status().isFailed());

            // when
            clientFactory.failConsumerCreationWith(null);
            registration.cancel();
            testSubject.subscribe(subscriber);

            // then
            assertThat(testSubject.status()).isEqualTo(new KafkaSourceStatus.Running(List.of()));
            await().atMost(Duration.ofSeconds(5)).until(() -> clientFactory.consumers().size() == 1);
        }

        @Test
        void reportsAnErrorOnThePollingThreadAsAFailure() {
            // given
            clientFactory.prepareConsumers(consumer -> consumer.schedulePollTask(() -> {
                throw new NoClassDefFoundError("com/example/MissingDeserializer");
            }));

            // when
            subscribeAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> testSubject.status().isFailed());
            assertThat(testSubject.status()).isInstanceOfSatisfying(
                    KafkaSourceStatus.Failed.class,
                    failed -> assertThat(failed.cause()).isInstanceOf(NoClassDefFoundError.class)
            );
        }

        @Test
        void reportsThePartitionsItStoppedConsumingWhileRunning() {
            // given - the committed offset of a partition no longer exists
            TopicPartition partition = new TopicPartition("parcels", 0);
            clientFactory.prepareConsumers(consumer -> consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(partition));
                consumer.updateBeginningOffsets(Map.of(partition, 5L));
                consumer.setPollException(new OffsetOutOfRangeException("retention gap", Map.of(partition, 2L)));
            }));

            // when
            subscribeAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(
                    () -> testSubject.status() instanceof KafkaSourceStatus.Running running
                            && !running.stoppedPartitions().isEmpty()
            );
            assertThat(testSubject.status()).isInstanceOfSatisfying(
                    KafkaSourceStatus.Running.class,
                    running -> assertThat(running.stoppedPartitions()).singleElement().satisfies(stopped -> {
                        assertThat(stopped.partition()).isEqualTo(partition);
                        assertThat(stopped.offset()).isEqualTo(2L);
                        assertThat(stopped.reason()).isEqualTo(StoppedPartition.Reason.RETENTION_GAP);
                    })
            );
            assertThat(testSubject.status().isFailed()).isFalse();
        }

        @Test
        void keepsRetryingWhileTheBootstrapServersDoNotResolve() {
            // given - the Kafka clients themselves, with a host name that is not registered in DNS
            KafkaClientFactory unresolvable = KafkaClientFactory.create(Map.of(
                    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "kafka-not-yet-there.invalid:9092"
            ));
            AtomicInteger attempts = new AtomicInteger();
            KafkaClientFactory counting = new KafkaClientFactory() {
                @Override
                public <K, V> Producer<K, V> createProducer(Map<String, Object> properties,
                                                            Serializer<K> keySerializer,
                                                            Serializer<V> valueSerializer) {
                    return unresolvable.createProducer(properties, keySerializer, valueSerializer);
                }

                @Override
                public <K, V> Consumer<K, V> createConsumer(Map<String, Object> properties,
                                                            Deserializer<K> keyDeserializer,
                                                            Deserializer<V> valueDeserializer) {
                    attempts.incrementAndGet();
                    return unresolvable.createConsumer(properties, keyDeserializer, valueDeserializer);
                }

                @Override
                public Admin createAdmin(Map<String, Object> properties) {
                    return unresolvable.createAdmin(properties);
                }
            };
            testSubject = new KafkaSubscribableEventSource(counting,
                                                           configuration,
                                                           UnitOfWorkTestUtils.SIMPLE_FACTORY,
                                                           new RecordingEntitlementManager());

            // when
            subscribeAndStart();

            // then - the consumer cannot be constructed, and is tried again rather than given up on
            await().atMost(Duration.ofSeconds(10)).until(() -> attempts.get() > 2);
            assertThat(testSubject.status()).isEqualTo(new KafkaSourceStatus.Running(List.of()));
        }

        @Test
        void keepsRetryingTransientFailuresWithoutReportingThem() {
            // given
            clientFactory.failConsumerCreationWith(new KafkaException("broker unreachable"));

            // when
            subscribeAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> clientFactory.consumerProperties().size() > 2);
            assertThat(testSubject.status()).isEqualTo(new KafkaSourceStatus.Running(List.of()));
        }

        @Test
        void isStoppedUntilStartedAndSubscribedAndAfterShutdown() {
            // given
            KafkaSourceStatus initially = testSubject.status();
            testSubject.start();
            KafkaSourceStatus withoutSubscriber = testSubject.status();

            // when
            testSubject.subscribe((events, context) -> FutureUtils.emptyCompletedFuture());
            KafkaSourceStatus running = testSubject.status();
            testSubject.shutdown().orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(initially).isEqualTo(new KafkaSourceStatus.Stopped());
            assertThat(withoutSubscriber).isEqualTo(new KafkaSourceStatus.Stopped());
            assertThat(running).isEqualTo(new KafkaSourceStatus.Running(List.of()));
            assertThat(testSubject.status()).isEqualTo(new KafkaSourceStatus.Stopped());
        }
    }

    @Nested
    class Subscribing {

        @Test
        void startsConsumingOnceStartedWhenSubscribedFirst() {
            // given
            testSubject.subscribe((events, context) -> FutureUtils.emptyCompletedFuture());
            await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2))
                   .until(() -> clientFactory.consumerProperties().isEmpty());

            // when
            testSubject.start();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> clientFactory.consumerProperties().size() == 1);
        }

        @Test
        void startsConsumingOnceSubscribedWhenStartedFirst() {
            // given
            testSubject.start();
            await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2))
                   .until(() -> clientFactory.consumerProperties().isEmpty());

            // when
            testSubject.subscribe((events, context) -> FutureUtils.emptyCompletedFuture());

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> clientFactory.consumerProperties().size() == 1);
        }

        @Test
        void startsTheConfiguredNumberOfConsumers() {
            // given
            testSubject = source(configuration.withConsumerCount(3));

            // when
            subscribeAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> clientFactory.consumerProperties().size() == 3);
            await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2))
                   .until(() -> clientFactory.consumerProperties().size() == 3);
        }

        @Test
        void subscribingTheSameSubscriberAgainStartsNoFurtherConsumers() {
            // given
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> subscriber =
                    (events, context) -> FutureUtils.emptyCompletedFuture();
            testSubject.subscribe(subscriber);
            testSubject.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> clientFactory.consumerProperties().size() == 1);

            // when
            Registration again = testSubject.subscribe(subscriber);

            // then
            assertThat(again).isNotNull();
            await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2))
                   .until(() -> clientFactory.consumerProperties().size() == 1);
        }

        @Test
        void cancellingTheSubscriptionStopsTheConsumers() {
            // given
            Registration registration =
                    testSubject.subscribe((events, context) -> FutureUtils.emptyCompletedFuture());
            testSubject.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> clientFactory.consumers().size() == 1);

            // when
            boolean cancelled = registration.cancel();

            // then
            assertThat(cancelled).isTrue();
            await().atMost(Duration.ofSeconds(10)).until(() -> clientFactory.consumers().getFirst().closed());
        }

        @Test
        void rejectsASecondSubscriber() {
            // given
            testSubject.subscribe((events, context) -> FutureUtils.emptyCompletedFuture());

            // when / then
            assertThatThrownBy(() -> testSubject.subscribe((events, context) -> FutureUtils.emptyCompletedFuture()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("billing");
        }
    }

    @Nested
    class Shutdown {

        @Test
        void shutdownCompletesOnceTheConsumersHaveStopped() {
            // given
            subscribeAndStart();
            await().atMost(Duration.ofSeconds(5)).until(() -> clientFactory.consumers().size() == 1);

            // when
            CompletableFuture<Void> stopped = testSubject.shutdown();

            // then
            assertThat(stopped).succeedsWithin(Duration.ofSeconds(30));
            assertThat(clientFactory.consumers().getFirst().closed()).isTrue();
        }
    }

    @Nested
    class DeadLetters {

        private static final TopicPartition PARTITION = new TopicPartition("parcels", 0);

        private void deliverOneEvent() {
            DefaultKafkaEventCodec codec = CodecTestFixtures.defaultCodec();
            ConsumerRecord<String, byte[]> record = ConsumedRecords.consumed(
                    codec.encode(EventTestUtils.asEventMessage("parcel dispatched"), "parcels", "p-1", null), 0, 0
            );
            clientFactory.prepareConsumers(created -> {
                @SuppressWarnings("unchecked")
                MockConsumer<String, byte[]> consumer = (MockConsumer<String, byte[]>) created;
                consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
                consumer.schedulePollTask(() -> {
                    consumer.rebalance(List.of(PARTITION));
                    consumer.addRecord(record);
                });
            });
        }

        private void subscribeFailingAndStart() {
            testSubject.subscribe((events, context) -> CompletableFuture.failedFuture(
                    new IllegalStateException("handler failed")
            ));
            testSubject.start();
        }

        @Test
        void deadLetterProducerBlocksTheSendingThreadForTenSecondsAtMostByDefault() {
            // given
            deliverOneEvent();
            testSubject = source(configuration.withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC));

            // when
            subscribeFailingAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> !clientFactory.producerProperties().isEmpty());
            assertThat(clientFactory.producerProperties().getFirst())
                    .containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        }

        @Test
        void deadLetterProducerIsCreatedForAClientNameOfItsOwn() {
            // given
            deliverOneEvent();
            testSubject = source(configuration.withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC));

            // when
            subscribeFailingAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> !clientFactory.producerProperties().isEmpty());
            assertThat(clientFactory.producerClientNames()).containsExactly("dlt");
        }

        @Test
        void deadLetterProducerUsesTheConfiguredSettingsAndSuffix() {
            // given
            deliverOneEvent();
            testSubject = source(configuration.withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC)
                                              .withDeadLetterTopicSuffix(".failed")
                                              .withProducerProperties(Map.of(ProducerConfig.MAX_BLOCK_MS_CONFIG, 500,
                                                                             ProducerConfig.ACKS_CONFIG, "all")));

            // when
            subscribeFailingAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(
                    () -> !clientFactory.producers().isEmpty()
                            && !clientFactory.producers().getFirst().history().isEmpty()
            );
            assertThat(clientFactory.producerProperties().getFirst())
                    .containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, 500)
                    .containsEntry(ProducerConfig.ACKS_CONFIG, "all");
            assertThat(clientFactory.producers().getFirst().history())
                    .first().extracting(ProducerRecord::topic).isEqualTo("parcels.failed");
        }

        @Test
        void deadLetterProducerIsClosedAtShutdown() {
            // given
            deliverOneEvent();
            testSubject = source(configuration.withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC));
            subscribeFailingAndStart();
            await().atMost(Duration.ofSeconds(5)).until(() -> !clientFactory.producers().isEmpty());

            // when
            testSubject.shutdown().orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(clientFactory.producers().getFirst().closed()).isTrue();
        }
    }

    @Nested
    class Processing {

        @Test
        void processesEventsOnTheConfiguredExecutorAndLeavesItRunningAtShutdown() {
            // given - an executor of named platform threads, and a consumer that delivers one event
            ExecutorService executor = Executors.newFixedThreadPool(
                    2, Thread.ofPlatform().name("application-executor-", 0).factory()
            );
            TopicPartition partition = new TopicPartition("parcels", 0);
            ProducerRecord<String, byte[]> produced = CodecTestFixtures.defaultCodec().encode(
                    EventTestUtils.asEventMessage("parcel-1"), "parcels", null, null
            );
            clientFactory.prepareConsumers(consumer -> consumer.schedulePollTask(() -> {
                MockConsumer<String, byte[]> typed = cast(consumer);
                typed.rebalance(List.of(partition));
                typed.updateBeginningOffsets(Map.of(partition, 0L));
                typed.addRecord(ConsumedRecords.consumed(produced, 0, 0));
            }));
            List<String> threads = new CopyOnWriteArrayList<>();
            testSubject = source(configuration.withExecutor(executor));
            testSubject.subscribe((events, context) -> {
                threads.add(Thread.currentThread().getName());
                return FutureUtils.emptyCompletedFuture();
            });

            try {
                // when
                testSubject.start();
                await().atMost(Duration.ofSeconds(5)).until(() -> !threads.isEmpty());
                testSubject.shutdown().orTimeout(30, TimeUnit.SECONDS).join();

                // then
                assertThat(threads).allMatch(name -> name.startsWith("application-executor-"));
                assertThat(executor.isShutdown()).isFalse();
            } finally {
                executor.shutdownNow();
            }
        }

        @SuppressWarnings("unchecked")
        private static MockConsumer<String, byte[]> cast(MockConsumer<?, ?> consumer) {
            return (MockConsumer<String, byte[]>) consumer;
        }
    }

    @Nested
    class Warnings {

        private Logger sourceLogger;
        private ListAppender appender;

        @BeforeEach
        void attachAppender() {
            sourceLogger = (Logger) LogManager.getLogger(KafkaSubscribableEventSource.class);
            appender = new ListAppender("KafkaSubscribableEventSourceTest");
            appender.start();
            sourceLogger.addAppender(appender);
        }

        @AfterEach
        void detachAppender() {
            sourceLogger.removeAppender(appender);
            appender.stop();
        }

        private List<String> warnings() {
            return appender.getEvents().stream()
                           .map(event -> event.getMessage().getFormattedMessage())
                           .toList();
        }

        @Test
        void warnsAboutAGroupIdentifierAndAutomaticCommitsInTheConsumerProperties() {
            // when
            source(configuration.withConsumerProperties(Map.of(ConsumerConfig.GROUP_ID_CONFIG, "other",
                                                               ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)));

            // then
            assertThat(warnings()).anyMatch(message -> message.contains("group.id [other]"))
                                  .anyMatch(message -> message.contains("enable.auto.commit=true"));
        }

        @Test
        void warnsAboutAPatternThatOnlyMatchesDeadLetterTopics() {
            // when
            source(KafkaSubscribableEventSourceConfiguration.create(
                    CodecTestFixtures.defaultCodec(),
                    TopicSubscription.pattern(Pattern.compile("orders\\..*\\.DLT")),
                    "reprocessing"
            ));

            // then
            assertThat(warnings()).anyMatch(message -> message.contains("dead-letter topic suffix [.DLT]"));
        }

        @Test
        void doesNotWarnAboutAPatternThatAlsoMatchesOtherTopics() {
            // when
            source(KafkaSubscribableEventSourceConfiguration.create(
                    CodecTestFixtures.defaultCodec(), TopicSubscription.pattern(Pattern.compile("orders\\..*")),
                    "billing"
            ));

            // then
            assertThat(warnings()).isEmpty();
        }
    }

    @Nested
    class ConsumerSettings {

        @Test
        void consumersJoinTheGroupWithoutAutomaticCommitsOrOffsetReset() {
            // when
            subscribeAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> !clientFactory.consumerProperties().isEmpty());
            assertThat(clientFactory.consumerProperties().getFirst())
                    .containsEntry(ConsumerConfig.GROUP_ID_CONFIG, "billing")
                    .containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                    .containsEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        }

        @Test
        void anExplicitOffsetResetSettingIsKept() {
            // given
            testSubject = source(configuration.withConsumerProperties(
                    Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
            ));

            // when
            subscribeAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> !clientFactory.consumerProperties().isEmpty());
            assertThat(clientFactory.consumerProperties().getFirst())
                    .containsEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        }

        @Test
        void everyConsumerGetsAGroupInstanceIdentifierOfItsOwn() {
            // given
            testSubject = source(configuration.withConsumerCount(3).withConsumerProperties(
                    Map.of(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "billing-host-1")
            ));

            // when
            subscribeAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> clientFactory.consumerProperties().size() == 3);
            assertThat(clientFactory.consumerProperties())
                    .extracting(properties -> properties.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG))
                    .containsExactlyInAnyOrder("billing-host-1-0", "billing-host-1-1", "billing-host-1-2");
        }

        @Test
        void everyConsumerIsCreatedForAClientNameOfItsOwn() {
            // given
            testSubject = source(configuration.withConsumerCount(3));

            // when
            subscribeAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> clientFactory.consumers().size() == 3);
            assertThat(clientFactory.consumerClientNames()).containsExactlyInAnyOrder("0", "1", "2");
        }

        @Test
        void aConsumerCreatedAnewAfterATransientFailureKeepsItsClientName() {
            // given - the first consumer loses its connection
            AtomicInteger created = new AtomicInteger();
            clientFactory.prepareConsumers(consumer -> {
                if (created.getAndIncrement() == 0) {
                    consumer.schedulePollTask(
                            () -> consumer.setPollException(new KafkaException("connection lost"))
                    );
                }
            });

            // when
            subscribeAndStart();

            // then - the consumer that replaces it is created for the same client name, so it keeps its client id
            await().atMost(Duration.ofSeconds(5)).until(() -> clientFactory.consumers().size() == 2);
            assertThat(clientFactory.consumerClientNames()).containsExactly("0", "0");
            assertThat(testSubject.status()).isInstanceOf(KafkaSourceStatus.Running.class);
        }

        @Test
        void aSingleConsumerKeepsTheGroupInstanceIdentifierAsItIs() {
            // given
            testSubject = source(configuration.withConsumerProperties(
                    Map.of(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "billing-host-1")
            ));

            // when
            subscribeAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> !clientFactory.consumerProperties().isEmpty());
            assertThat(clientFactory.consumerProperties().getFirst())
                    .containsEntry(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "billing-host-1");
        }

        @Test
        void consumersReadCommittedRecordsOnlyByDefault() {
            // when
            subscribeAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> !clientFactory.consumerProperties().isEmpty());
            assertThat(clientFactory.consumerProperties().getFirst())
                    .containsEntry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }

        @Test
        void anExplicitIsolationLevelIsKept() {
            // given
            testSubject = source(configuration.withConsumerProperties(
                    Map.of(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted")
            ));

            // when
            subscribeAndStart();

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> !clientFactory.consumerProperties().isEmpty());
            assertThat(clientFactory.consumerProperties().getFirst())
                    .containsEntry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        }
    }
}
