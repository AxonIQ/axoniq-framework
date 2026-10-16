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
import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import io.axoniq.framework.kafka.codec.LegacyKafkaEventCodec;
import io.axoniq.framework.kafka.codec.UndecodableRecordPolicy;
import io.axoniq.framework.kafka.common.KafkaResources;
import io.axoniq.framework.kafka.common.RetentionGapPolicy;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.framework.kafka.utils.ConsumedRecords;
import io.axoniq.framework.kafka.utils.RecordingEntitlementManager;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.axonframework.common.FutureUtils;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static io.axoniq.framework.kafka.codec.CodecTestFixtures.header;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class GroupConsumerTest {

    private static final String TOPIC = "parcels";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final TopicPartition OTHER_PARTITION = new TopicPartition(TOPIC, 1);

    private final DefaultKafkaEventCodec codec =
            CodecTestFixtures.defaultCodec();
    private final TestConsumer consumer = new TestConsumer();
    private final MockProducer<String, byte[]> deadLetterProducer =
            new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer());
    private final RecordingEntitlementManager entitlementManager = new RecordingEntitlementManager();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final TrackingExecutor processingExecutor = new TrackingExecutor(executor);
    private final List<Long> processed = new CopyOnWriteArrayList<>();
    private final Map<Integer, List<Long>> processedByPartition = new ConcurrentHashMap<>();
    private final List<ProcessingContext> contexts = new CopyOnWriteArrayList<>();
    private final List<String> processingThreads = new CopyOnWriteArrayList<>();
    private volatile int failingPartition = -1;
    private final Map<Long, AtomicInteger> attempts = new ConcurrentHashMap<>();
    private volatile Function<Long, CompletableFuture<?>> handler = offset -> FutureUtils.emptyCompletedFuture();
    private volatile BiFunction<Integer, Long, CompletableFuture<?>> partitionHandler =
            (partition, offset) -> handler.apply(offset);
    private KafkaEventCodec<byte[]> recordCodec = codec;
    private @Nullable GroupConsumer<byte[]> testSubject;

    record ParcelDispatched(String parcelId) {

    }

    /**
     * An {@link Executor} handing tasks to a delegate, which tracks the tasks that have not finished and can reject a
     * task, as a bounded executor does when it is saturated.
     */
    static class TrackingExecutor implements Executor {

        private final Executor delegate;
        private final AtomicInteger submitted = new AtomicInteger();
        private final AtomicInteger unfinished = new AtomicInteger();
        /**
         * The number of the task to reject, counting from 1, or 0 to accept all tasks.
         */
        volatile int rejectedTask;

        TrackingExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable command) {
            if (submitted.incrementAndGet() == rejectedTask) {
                throw new RejectedExecutionException("task " + rejectedTask + " is rejected");
            }
            unfinished.incrementAndGet();
            try {
                delegate.execute(() -> {
                    try {
                        command.run();
                    } finally {
                        unfinished.decrementAndGet();
                    }
                });
            } catch (RuntimeException e) {
                unfinished.decrementAndGet();
                throw e;
            }
        }

        boolean idle() {
            return unfinished.get() == 0;
        }
    }

    /**
     * A {@link MockConsumer} that records the offsets committed and the offsets it is moved to with {@code seek}, can
     * fail commits, can leave a wake-up pending until the next commit, as a real consumer does when it is woken up
     * outside of {@code poll}, and can revoke its partitions when it is closed, as a real consumer does. It can also
     * hold the polling thread in its next call to {@link #assignment()}, to stop the member at a precise point.
     */
    static class TestConsumer extends MockConsumer<String, byte[]> {

        final List<Long> commits = new CopyOnWriteArrayList<>();
        final List<Map<TopicPartition, Long>> commitRequests = new CopyOnWriteArrayList<>();
        final Map<TopicPartition, List<Long>> commitsByPartition = new ConcurrentHashMap<>();
        final Map<TopicPartition, List<Long>> seeksByPartition = new ConcurrentHashMap<>();
        final AtomicInteger failingCommits = new AtomicInteger();
        final AtomicBoolean pendingWakeup = new AtomicBoolean();
        final AtomicInteger polls = new AtomicInteger();
        volatile boolean wakeUpOutsideOfPoll;
        volatile boolean revokeOnClose;
        final CompletableFuture<Void> assignmentGateReached = new CompletableFuture<>();
        private volatile @Nullable CompletableFuture<Void> assignmentGate;
        private volatile @Nullable ConsumerRebalanceListener listener;

        TestConsumer() {
            this(OffsetResetStrategy.EARLIEST);
        }

        // The constructor taking a String, which kafka-clients 4 prefers, does not exist in the 3.x line.
        @SuppressWarnings("deprecation")
        TestConsumer(OffsetResetStrategy offsetResetStrategy) {
            super(offsetResetStrategy);
        }

        ConsumerRebalanceListener listener() {
            return Objects.requireNonNull(listener);
        }

        /**
         * Holds the polling thread in its next call to {@link #assignment()} until the given {@code gate} completes,
         * completing {@link #assignmentGateReached} once it got there.
         */
        void holdPollingThreadInNextAssignmentCall(CompletableFuture<Void> gate) {
            assignmentGate = gate;
        }

        @Override
        public Set<TopicPartition> assignment() {
            CompletableFuture<Void> gate = assignmentGate;
            if (gate != null && Thread.currentThread().getName().equals("group-consumer-test")) {
                assignmentGate = null;
                assignmentGateReached.complete(null);
                gate.orTimeout(10, TimeUnit.SECONDS).join();
            }
            return super.assignment();
        }

        @Override
        public synchronized void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
            this.listener = listener;
            super.subscribe(topics, listener);
        }

        @Override
        public synchronized void seek(TopicPartition partition, long offset) {
            seeksByPartition.computeIfAbsent(partition, p -> new CopyOnWriteArrayList<>()).add(offset);
            super.seek(partition, offset);
        }

        @Override
        public synchronized ConsumerRecords<String, byte[]> poll(Duration timeout) {
            polls.incrementAndGet();
            return super.poll(timeout);
        }

        @Override
        @SuppressWarnings("deprecation")
        public void close(Duration timeout) {
            ConsumerRebalanceListener current = listener;
            if (revokeOnClose && current != null) {
                current.onPartitionsRevoked(assignment());
            }
            super.close(timeout);
        }

        @Override
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
            if (pendingWakeup.getAndSet(false)) {
                throw new WakeupException();
            }
            if (failingCommits.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new CommitFailedException();
            }
            super.commitSync(offsets, timeout);
            Map<TopicPartition, Long> request = new HashMap<>();
            offsets.forEach((partition, offset) -> request.put(partition, offset.offset()));
            commitRequests.add(request);
            offsets.forEach((partition, offset) -> commitsByPartition
                    .computeIfAbsent(partition, p -> new CopyOnWriteArrayList<>())
                    .add(offset.offset()));
            OffsetAndMetadata own = offsets.get(PARTITION);
            if (own != null) {
                commits.add(own.offset());
            }
        }

        @Override
        public void wakeup() {
            // Not synchronized itself, but only with wakeUpOutsideOfPoll does a stop request not wait for a polling
            // thread that holds the consumer's lock, for example while it waits for events in progress inside a
            // rebalance listener: MockConsumer.wakeup() is synchronized.
            if (wakeUpOutsideOfPoll) {
                pendingWakeup.set(true);
            } else {
                super.wakeup();
            }
        }
    }

    @AfterEach
    void tearDown() {
        if (testSubject != null) {
            testSubject.requestStop();
            assertThat(testSubject.awaitStop(Duration.ofSeconds(10))).isTrue();
        }
        executor.shutdownNow();
    }

    private KafkaSubscribableEventSourceConfiguration configuration() {
        return KafkaSubscribableEventSourceConfiguration.create(codec, TopicSubscription.topics(TOPIC), "billing")
                                                        .withPollTimeout(Duration.ofMillis(10))
                                                        .withRevocationTimeout(Duration.ofMillis(200))
                                                        .withRetryBackoff(Duration.ofMillis(10),
                                                                          Duration.ofMillis(50));
    }

    private void start(KafkaSubscribableEventSourceConfiguration configuration) {
        start(configuration, () -> consumer);
    }

    private void start(KafkaSubscribableEventSourceConfiguration configuration,
                       Supplier<Consumer<String, byte[]>> consumers) {
        start(configuration, consumers, () -> deadLetterProducer);
    }

    private void start(KafkaSubscribableEventSourceConfiguration configuration,
                       Supplier<Consumer<String, byte[]>> consumers,
                       Supplier<Producer<String, byte[]>> deadLetterProducers) {
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L, OTHER_PARTITION, 0L));
        testSubject = new GroupConsumer<>(
                consumers,
                configuration,
                recordCodec,
                (events, context) -> {
                    ProcessingContext eventContext = Objects.requireNonNull(context);
                    contexts.add(eventContext);
                    processingThreads.add(Thread.currentThread().getName());
                    long offset = eventContext.getResource(KafkaResources.OFFSET);
                    int partition = eventContext.getResource(KafkaResources.TOPIC_PARTITION).partition();
                    processedByPartition.computeIfAbsent(partition, p -> new CopyOnWriteArrayList<>()).add(offset);
                    if (partition == failingPartition) {
                        return CompletableFuture.failedFuture(new IllegalStateException("partition fails"));
                    }
                    processed.add(offset);
                    attempts.computeIfAbsent(offset, o -> new AtomicInteger()).incrementAndGet();
                    return partitionHandler.apply(partition, offset);
                },
                UnitOfWorkTestUtils.SIMPLE_FACTORY,
                processingExecutor,
                new DeadLetterTopicWriter<>(deadLetterProducers, configuration.deadLetterTopicSuffix()),
                entitlementManager
        );
        testSubject.start("group-consumer-test");
    }

    @SafeVarargs
    private void assignWithRecords(ConsumerRecord<String, byte[]>... records) {
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(PARTITION));
            for (ConsumerRecord<String, byte[]> record : records) {
                consumer.addRecord(record);
            }
        });
    }

    private List<Long> processedOf(TopicPartition partition) {
        return processedByPartition.getOrDefault(partition.partition(), List.of());
    }

    private ConsumerRecord<String, byte[]> malformedRecord(long offset) {
        ConsumerRecord<String, byte[]> malformed = eventRecord(offset);
        CodecTestFixtures.makeTimestampUnreadable(malformed.headers());
        return malformed;
    }

    private ConsumerRecord<String, byte[]> eventRecordAt(long offset, long timestamp) {
        ConsumerRecord<String, byte[]> record = eventRecord(offset);
        return new ConsumerRecord<>(TOPIC, PARTITION.partition(), offset, timestamp, TimestampType.CREATE_TIME, -1, -1,
                                    record.key(), record.value(), record.headers(), Optional.empty());
    }

    private ConsumerRecord<String, byte[]> eventRecord(long offset) {
        return eventRecord(PARTITION, offset);
    }

    private ConsumerRecord<String, byte[]> eventRecord(TopicPartition partition, long offset) {
        ProducerRecord<String, byte[]> produced = codec.encode(
                EventTestUtils.asEventMessage(new ParcelDispatched("p-" + offset)), TOPIC, null, null
        );
        return ConsumedRecords.consumed(produced, partition.partition(), offset);
    }

    @Nested
    class Processing {

        @Test
        void pausesThePartitionWhileItsRecordsAreProcessedAndCommitsAfterwards() {
            // given
            CompletableFuture<Void> processing = new CompletableFuture<>();
            handler = offset -> processing;
            assignWithRecords(eventRecord(0));

            // when
            start(configuration());

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L)));
            assertThat(consumer.paused()).containsExactly(PARTITION);
            assertThat(consumer.commits).isEmpty();

            // when
            processing.complete(null);

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.equals(List.of(1L)));
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.paused().isEmpty());
        }

        @Test
        void failedCommitDoesNotStopTheMember() {
            // given
            consumer.failingCommits.set(1);
            assignWithRecords(eventRecord(0));
            consumer.schedulePollTask(() -> consumer.addRecord(eventRecord(1)));

            // when
            start(configuration());

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L, 1L)));
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(2L));
            assertThat(consumer.commits).doesNotContain(1L);
        }

        @Test
        void memberRejoinsWithANewConsumerAfterAnUnexpectedFailure() {
            // given - the first consumer fails, the second one delivers a record
            TestConsumer failing = new TestConsumer();
            failing.schedulePollTask(() -> {
                failing.rebalance(List.of(PARTITION));
                failing.setPollException(new KafkaException("unexpected"));
            });
            assignWithRecords(eventRecord(0));
            Queue<Consumer<String, byte[]>> consumers = new ConcurrentLinkedQueue<>(List.of(failing, consumer));

            // when
            start(configuration(), consumers::remove);

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L)));
            assertThat(failing.closed()).isTrue();
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.equals(List.of(1L)));
            assertThat(testSubject.failure()).isNotDone();
        }

        @Test
        void neverProcessesOnThePollingThread() {
            // given
            assignWithRecords(eventRecord(0), eventRecord(1));

            // when
            start(configuration());

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.size() == 2);
            assertThat(processingThreads).doesNotContain("group-consumer-test");
        }

        @Test
        void resourcesTheCodecDecodedReachTheProcessingContext() {
            // given - a codec writing tags, and the legacy codec writing aggregate headers
            DefaultKafkaEventCodec tagging = codec.withTagResolver(event -> Set.of(Tag.of("parcelId", "p-0")));
            recordCodec = new LegacyKafkaEventCodec(tagging, new DelegatingEventConverter(new JacksonConverter()));
            ProducerRecord<String, byte[]> produced = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    context -> CompletableFuture.completedFuture(recordCodec.encode(
                            EventTestUtils.asEventMessage(new ParcelDispatched("p-0")), TOPIC, null,
                            context.withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "parcel-0")
                                   .withResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY, 4L)
                                   .withResource(LegacyResources.AGGREGATE_TYPE_KEY, "Parcel")
                    ))
            ).orTimeout(5, TimeUnit.SECONDS).join();
            assignWithRecords(ConsumedRecords.consumed(produced, PARTITION.partition(), 0));

            // when
            start(configuration());

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> contexts.size() == 1);
            ProcessingContext context = contexts.getFirst();
            assertThat(Tag.fromContext(context)).hasValue(Set.of(Tag.of("parcelId", "p-0")));
            assertThat(context.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)).isEqualTo("parcel-0");
            assertThat(context.getResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY)).isEqualTo(4L);
        }
    }

    @Nested
    class TopicPatterns {

        @Test
        void patternSubscriptionLeavesOutTheDeadLetterTopics() {
            // given - a pattern that also matches the dead-letter topics of the topics it matches
            consumer.updatePartitions("orders.created", List.of(partitionInfo("orders.created")));
            consumer.updatePartitions("orders.created.DLT", List.of(partitionInfo("orders.created.DLT")));
            consumer.updatePartitions("orders.created.DLT.DLT", List.of(partitionInfo("orders.created.DLT.DLT")));
            KafkaSubscribableEventSourceConfiguration configuration =
                    KafkaSubscribableEventSourceConfiguration.create(codec,
                                                                     TopicSubscription.pattern(
                                                                             Pattern.compile("orders\\..*")
                                                                     ),
                                                                     "billing");

            // when
            start(configuration);

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> !consumer.subscription().isEmpty());
            assertThat(consumer.subscription()).containsExactly("orders.created");
        }

        @Test
        void patternSubscriptionLeavesOutTheTopicsWithACustomDeadLetterSuffix() {
            // given
            consumer.updatePartitions("orders.created", List.of(partitionInfo("orders.created")));
            consumer.updatePartitions("orders.created-failed", List.of(partitionInfo("orders.created-failed")));
            KafkaSubscribableEventSourceConfiguration configuration =
                    KafkaSubscribableEventSourceConfiguration.create(codec,
                                                                     TopicSubscription.pattern(
                                                                             Pattern.compile("orders.*")
                                                                     ),
                                                                     "billing")
                                                             .withDeadLetterTopicSuffix("-failed");

            // when
            start(configuration);

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> !consumer.subscription().isEmpty());
            assertThat(consumer.subscription()).containsExactly("orders.created");
        }

        private static PartitionInfo partitionInfo(String topic) {
            return new PartitionInfo(topic, 0, null, new Node[0], new Node[0]);
        }
    }

    @Nested
    class MultiplePartitions {

        @Test
        void retryingTheRecordOfOnePartitionLeavesTheOtherPartitionsFlowing() {
            // given
            failingPartition = PARTITION.partition();
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(PARTITION, OTHER_PARTITION));
                consumer.addRecord(eventRecord(PARTITION, 0));
                consumer.addRecord(eventRecord(OTHER_PARTITION, 0));
                consumer.addRecord(eventRecord(OTHER_PARTITION, 1));
            });

            // when
            start(configuration());

            // then
            await().atMost(Duration.ofSeconds(5)).until(
                    () -> consumer.commitsByPartition.getOrDefault(OTHER_PARTITION, List.of()).contains(2L)
            );
            assertThat(processedByPartition.get(OTHER_PARTITION.partition())).containsExactly(0L, 1L);
            assertThat(consumer.commitsByPartition.getOrDefault(PARTITION, List.of())).allMatch(offset -> offset == 0L);
        }

        @Test
        void stoppingAtTheRecordOfOnePartitionLeavesTheOtherPartitionsFlowing() {
            // given
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(PARTITION, OTHER_PARTITION));
                consumer.addRecord(malformedRecord(0));
                consumer.addRecord(eventRecord(OTHER_PARTITION, 0));
                consumer.addRecord(eventRecord(OTHER_PARTITION, 1));
            });

            // when
            start(configuration().withUndecodableRecordPolicy(UndecodableRecordPolicy.FAIL));

            // then
            await().atMost(Duration.ofSeconds(5)).until(
                    () -> consumer.commitsByPartition.getOrDefault(OTHER_PARTITION, List.of()).contains(2L)
            );
            assertThat(consumer.paused()).contains(PARTITION).doesNotContain(OTHER_PARTITION);
            assertThat(processedByPartition).doesNotContainKey(PARTITION.partition());
        }
    }

    @Nested
    class RetryBackoff {

        private Logger groupConsumerLogger;
        private ListAppender appender;

        @BeforeEach
        void attachAppender() {
            groupConsumerLogger = (Logger) LogManager.getLogger(GroupConsumer.class);
            appender = new ListAppender("GroupConsumerTest");
            appender.start();
            groupConsumerLogger.addAppender(appender);
        }

        @AfterEach
        void detachAppender() {
            groupConsumerLogger.removeAppender(appender);
            appender.stop();
        }

        private List<String> retryDelays() {
            return appender.getEvents().stream()
                           .map(event -> event.getMessage().getFormattedMessage())
                           .filter(message -> message.contains("Retrying in"))
                           .map(message -> message.substring(message.indexOf("Retrying in") + 12,
                                                             message.lastIndexOf(" ms")))
                           .toList();
        }

        @Test
        void growsExponentiallyUpToTheMaximumAndStartsAfreshForTheNextRecord() {
            // given - the first record fails five times, the second one once
            handler = offset -> {
                int attempt = attempts.get(offset).get();
                if (attempt > (offset == 0 ? 5 : 1)) {
                    if (offset == 0) {
                        consumer.schedulePollTask(() -> consumer.addRecord(eventRecord(1)));
                    }
                    return FutureUtils.emptyCompletedFuture();
                }
                // the member seeks back to the failed record; the mock consumer needs it again to redeliver it
                consumer.schedulePollTask(() -> consumer.addRecord(eventRecord(offset)));
                return CompletableFuture.failedFuture(new IllegalStateException("attempt " + attempt + " fails"));
            };
            assignWithRecords(eventRecord(0));

            // when
            start(configuration().withRetryBackoff(Duration.ofMillis(10), Duration.ofMillis(50)));

            // then
            await().atMost(Duration.ofSeconds(10)).until(() -> consumer.commits.contains(2L));
            assertThat(retryDelays()).containsExactly("10", "20", "40", "50", "50", "10");
        }

        @Test
        void startsAfreshForTheNextRecordOfTheBatchThatRetriesTheFailedRecord() {
            // given - the first record fails twice, and is redelivered together with the second one, which fails once
            handler = offset -> {
                int attempt = attempts.get(offset).get();
                if (attempt > (offset == 0 ? 2 : 1)) {
                    return FutureUtils.emptyCompletedFuture();
                }
                // the member seeks back to the failed record; the mock consumer needs the records again to redeliver
                consumer.schedulePollTask(() -> {
                    consumer.addRecord(eventRecord(offset));
                    if (offset == 0) {
                        consumer.addRecord(eventRecord(1));
                    }
                });
                return CompletableFuture.failedFuture(new IllegalStateException("attempt " + attempt + " fails"));
            };
            assignWithRecords(eventRecord(0));

            // when
            start(configuration().withRetryBackoff(Duration.ofMillis(10), Duration.ofMillis(50)));

            // then - the second record's failure is its first attempt
            await().atMost(Duration.ofSeconds(10)).until(() -> consumer.commits.contains(2L));
            assertThat(retryDelays()).containsExactly("10", "20", "10");
        }
    }

    @Nested
    class FencedStaticMembers {

        private Logger groupConsumerLogger;
        private ListAppender appender;

        @BeforeEach
        void attachAppender() {
            groupConsumerLogger = (Logger) LogManager.getLogger(GroupConsumer.class);
            appender = new ListAppender("GroupConsumerTest");
            appender.start();
            groupConsumerLogger.addAppender(appender);
        }

        @AfterEach
        void detachAppender() {
            groupConsumerLogger.removeAppender(appender);
            appender.stop();
        }

        @Test
        void fencedMemberNamesASharedGroupInstanceIdAsTheLikelyCause() {
            // given - another member joined with the same group.instance.id, for example in another instance
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(PARTITION));
                consumer.setPollException(new FencedInstanceIdException("fenced by another member"));
            });

            // when
            start(configuration());

            // then - the member keeps retrying, and the log names the likely cause
            await().atMost(Duration.ofSeconds(5)).until(
                    () -> appender.getEvents().stream()
                                  .map(event -> event.getMessage().getFormattedMessage())
                                  .anyMatch(message -> message.contains("same group.instance.id"))
            );
            assertThat(testSubject.failure()).isNotDone();
        }
    }

    @Nested
    class PermanentFailures {

        @Test
        void memberStopsWhenItsConsumerCannotBeCreatedWithTheGivenSettings() {
            // given - the Kafka client wraps invalid settings
            AtomicInteger created = new AtomicInteger();
            Supplier<Consumer<String, byte[]>> consumers = () -> {
                created.incrementAndGet();
                throw new KafkaException("Failed to construct kafka consumer",
                                         new ConfigException("session.timeout.ms", -1, "Value must be at least 0"));
            };

            // when
            start(configuration(), consumers);

            // then - the failure is reported, and the member does not retry
            assertThat(testSubject.failure()).failsWithin(Duration.ofSeconds(5))
                                             .withThrowableThat()
                                             .havingRootCause()
                                             .isInstanceOf(ConfigException.class);
            assertThat(testSubject.awaitStop(Duration.ofSeconds(5))).isTrue();
            assertThat(created).hasValue(1);
        }

        @Test
        void memberStopsWhenItIsNotAuthorizedToReadTheTopic() {
            // given
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(PARTITION));
                consumer.setPollException(new TopicAuthorizationException(Set.of(TOPIC)));
            });
            AtomicInteger created = new AtomicInteger();

            // when
            start(configuration(), () -> {
                created.incrementAndGet();
                return consumer;
            });

            // then
            assertThat(testSubject.failure()).failsWithin(Duration.ofSeconds(5))
                                             .withThrowableThat()
                                             .havingRootCause()
                                             .isInstanceOf(TopicAuthorizationException.class);
            assertThat(testSubject.awaitStop(Duration.ofSeconds(5))).isTrue();
            assertThat(consumer.closed()).isTrue();
            assertThat(created).hasValue(1);
        }

        @Test
        void memberStopsWhenItsAuthenticationFails() {
            // given - in the kafka-clients 3.x line, authentication failures are no configuration exceptions
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(PARTITION));
                consumer.setPollException(new SaslAuthenticationException("Authentication failed: invalid password"));
            });
            AtomicInteger created = new AtomicInteger();

            // when
            start(configuration(), () -> {
                created.incrementAndGet();
                return consumer;
            });

            // then
            assertThat(testSubject.failure()).failsWithin(Duration.ofSeconds(5))
                                             .withThrowableThat()
                                             .havingRootCause()
                                             .isInstanceOf(SaslAuthenticationException.class);
            assertThat(testSubject.awaitStop(Duration.ofSeconds(5))).isTrue();
            assertThat(created).hasValue(1);
        }

        @Test
        void memberKeepsRetryingTransientFailures() {
            // given - consumers fail to be created twice, as while the broker is unreachable
            AtomicInteger attemptsToCreate = new AtomicInteger();
            assignWithRecords(eventRecord(0));
            Supplier<Consumer<String, byte[]>> consumers = () -> {
                if (attemptsToCreate.incrementAndGet() <= 2) {
                    throw new KafkaException("broker unreachable");
                }
                return consumer;
            };

            // when
            start(configuration(), consumers);

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L)));
            assertThat(attemptsToCreate).hasValue(3);
            assertThat(testSubject.failure()).isNotDone();
        }

        @Test
        void memberKeepsRetryingWhileTheBootstrapServersDoNotResolve() {
            // given - the Kafka cluster's host names are not registered in DNS yet when the first consumers are created
            AtomicInteger attemptsToCreate = new AtomicInteger();
            assignWithRecords(eventRecord(0));
            Supplier<Consumer<String, byte[]>> consumers = () -> {
                if (attemptsToCreate.incrementAndGet() <= 2) {
                    throw new KafkaException(
                            "Failed to construct kafka consumer",
                            new ConfigException("No resolvable bootstrap urls given in bootstrap.servers")
                    );
                }
                return consumer;
            };

            // when
            start(configuration(), consumers);

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L)));
            assertThat(attemptsToCreate).hasValue(3);
            assertThat(testSubject.failure()).isNotDone();
        }

        @Test
        void memberStopsWithTheFailureWhenThePollingThreadThrowsAnError() {
            // given - a class that is missing at runtime, for example
            consumer.schedulePollTask(() -> {
                throw new NoClassDefFoundError("com/example/MissingDeserializer");
            });

            // when
            start(configuration());

            // then
            assertThat(testSubject.failure()).failsWithin(Duration.ofSeconds(5))
                                             .withThrowableThat()
                                             .havingRootCause()
                                             .isInstanceOf(NoClassDefFoundError.class);
            assertThat(testSubject.awaitStop(Duration.ofSeconds(5))).isTrue();
            assertThat(consumer.closed()).isTrue();
        }

        @Test
        void failureCompletesNormallyWhenTheMemberIsStopped() {
            // given
            start(configuration());

            // when
            testSubject.requestStop();

            // then
            assertThat(testSubject.failure()).succeedsWithin(Duration.ofSeconds(5));
        }
    }

    @Nested
    class StartingPositions {

        private final TestConsumer resumingConsumer = new TestConsumer(OffsetResetStrategy.NONE);

        @Test
        void newConsumerGroupStartsAtTheEarliestRecord() {
            // given - no committed offset, and records from offset 3 on
            resumingConsumer.schedulePollTask(() -> {
                resumingConsumer.rebalance(List.of(PARTITION));
                resumingConsumer.updateBeginningOffsets(Map.of(PARTITION, 3L));
                resumingConsumer.addRecord(eventRecord(3));
                resumingConsumer.addRecord(eventRecord(4));
            });

            // when
            start(configuration(), () -> resumingConsumer);

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(3L, 4L)));
            await().atMost(Duration.ofSeconds(5)).until(() -> resumingConsumer.commits.contains(5L));
        }

        @Test
        void newConsumerGroupStartsAtTheEndWhenConfiguredTo() {
            // given - no committed offset, records at offsets 3 and 4, and one published after the group started
            resumingConsumer.schedulePollTask(() -> {
                resumingConsumer.rebalance(List.of(PARTITION));
                resumingConsumer.updateBeginningOffsets(Map.of(PARTITION, 3L));
                resumingConsumer.updateEndOffsets(Map.of(PARTITION, 5L));
                resumingConsumer.addRecord(eventRecord(3));
                resumingConsumer.addRecord(eventRecord(4));
            });
            resumingConsumer.schedulePollTask(() -> resumingConsumer.addRecord(eventRecord(5)));

            // when
            start(configuration().withInitialPosition(InitialPosition.LATEST), () -> resumingConsumer);

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> resumingConsumer.commits.contains(6L));
            assertThat(processed).containsExactly(5L);
        }

        @Test
        void newConsumerGroupStartingAtTheEndCommitsThatPositionRightAway() {
            // given - no committed offset, and the end of the partition at offset 5
            resumingConsumer.schedulePollTask(() -> {
                resumingConsumer.rebalance(List.of(PARTITION));
                resumingConsumer.updateBeginningOffsets(Map.of(PARTITION, 3L));
                resumingConsumer.updateEndOffsets(Map.of(PARTITION, 5L));
            });

            // when
            start(configuration().withInitialPosition(InitialPosition.LATEST), () -> resumingConsumer);

            // then - a rebalance before the first record continues from there, not from the end at that time
            await().atMost(Duration.ofSeconds(5)).until(() -> resumingConsumer.commits.equals(List.of(5L)));
            assertThat(processed).isEmpty();
        }

        @Test
        void resumingConsumerGroupStopsAtARetentionGapByDefault() {
            // given - the group committed offset 2, but retention deleted everything before offset 5
            scheduleRecordsFromOffsetFiveAfterACommitAtTwo();

            // when
            start(configuration(), () -> resumingConsumer);

            // then - the partition stops, and nothing is committed or processed
            await().atMost(Duration.ofSeconds(5)).until(() -> resumingConsumer.paused().contains(PARTITION));
            await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
                   .until(() -> processed.isEmpty() && resumingConsumer.paused().contains(PARTITION));
            assertThat(resumingConsumer.commits).isEmpty();
            assertThat(testSubject.failure()).isNotDone();
        }

        @Test
        void resumingConsumerGroupContinuesFromTheEarliestRecordWhenConfiguredTo() {
            // given
            scheduleRecordsFromOffsetFiveAfterACommitAtTwo();

            // when
            start(configuration().withRetentionGapPolicy(RetentionGapPolicy.RESET_TO_EARLIEST),
                  () -> resumingConsumer);

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(5L, 6L)));
            await().atMost(Duration.ofSeconds(5)).until(() -> resumingConsumer.commits.contains(7L));
        }

        private void scheduleRecordsFromOffsetFiveAfterACommitAtTwo() {
            resumingConsumer.schedulePollTask(() -> {
                // subscribing clears the mock's committed offsets, so the earlier commit is made here; the
                // single-argument commitSync is not recorded by the test consumer
                resumingConsumer.commitSync(Map.of(PARTITION, new OffsetAndMetadata(2L)));
                resumingConsumer.rebalance(List.of(PARTITION));
                resumingConsumer.updateBeginningOffsets(Map.of(PARTITION, 5L));
                resumingConsumer.addRecord(eventRecord(5));
                resumingConsumer.addRecord(eventRecord(6));
            });
        }
    }

    @Nested
    class StoppedPartitions {

        private final TestConsumer resumingConsumer = new TestConsumer(OffsetResetStrategy.NONE);

        @Test
        void reportsAPartitionStoppedAtAnUndecodableRecord() {
            // given
            assignWithRecords(eventRecord(0), malformedRecord(1));

            // when
            start(configuration().withUndecodableRecordPolicy(UndecodableRecordPolicy.FAIL));

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> !testSubject.stoppedPartitions().isEmpty());
            assertThat(testSubject.stoppedPartitions()).singleElement().satisfies(stopped -> {
                assertThat(stopped.partition()).isEqualTo(PARTITION);
                assertThat(stopped.offset()).isEqualTo(1L);
                assertThat(stopped.reason()).isEqualTo(StoppedPartition.Reason.UNDECODABLE_RECORD);
                assertThat(stopped.cause()).isNotNull();
            });
        }

        @Test
        void reportsAPartitionStoppedAtARetentionGap() {
            // given - the group committed offset 2, but retention deleted everything before offset 5
            resumingConsumer.schedulePollTask(() -> {
                resumingConsumer.commitSync(Map.of(PARTITION, new OffsetAndMetadata(2L)));
                resumingConsumer.rebalance(List.of(PARTITION));
                resumingConsumer.updateBeginningOffsets(Map.of(PARTITION, 5L));
                resumingConsumer.addRecord(eventRecord(5));
            });

            // when
            start(configuration(), () -> resumingConsumer);

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> !testSubject.stoppedPartitions().isEmpty());
            assertThat(testSubject.stoppedPartitions()).singleElement().satisfies(stopped -> {
                assertThat(stopped.partition()).isEqualTo(PARTITION);
                assertThat(stopped.offset()).isEqualTo(2L);
                assertThat(stopped.reason()).isEqualTo(StoppedPartition.Reason.RETENTION_GAP);
            });
        }

        @Test
        void reportsAPartitionWhileItsRecordIsRetriedAndNoLongerOnceItSucceeds() {
            // given - the record fails three times
            CompletableFuture<Void> fourthAttempt = new CompletableFuture<>();
            handler = offset -> {
                int attempt = attempts.get(offset).get();
                if (attempt > 3) {
                    return fourthAttempt;
                }
                consumer.schedulePollTask(() -> consumer.addRecord(eventRecord(offset)));
                return CompletableFuture.failedFuture(new IllegalStateException("attempt " + attempt + " fails"));
            };
            assignWithRecords(eventRecord(0));

            // when
            start(configuration());

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.size() == 4);
            assertThat(testSubject.stoppedPartitions()).singleElement().satisfies(stopped -> {
                assertThat(stopped.partition()).isEqualTo(PARTITION);
                assertThat(stopped.offset()).isEqualTo(0L);
                assertThat(stopped.reason()).isEqualTo(StoppedPartition.Reason.RETRYING);
                assertThat(stopped.attempts()).isEqualTo(3);
                assertThat(stopped.cause()).hasMessage("attempt 3 fails");
            });

            // when
            fourthAttempt.complete(null);

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> testSubject.stoppedPartitions().isEmpty());
        }

        @Test
        void noLongerReportsAPartitionThatMovedToAnotherConsumer() {
            // given
            assignWithRecords(malformedRecord(0));
            start(configuration().withUndecodableRecordPolicy(UndecodableRecordPolicy.FAIL));
            await().atMost(Duration.ofSeconds(5)).until(() -> !testSubject.stoppedPartitions().isEmpty());

            // when
            consumer.schedulePollTask(() -> consumer.rebalance(List.of()));

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> testSubject.stoppedPartitions().isEmpty());
        }
    }

    @Nested
    class Revocation {

        @Test
        void revokedPartitionStopsProcessingItsBatch() {
            // given - the first record is still being processed when the partition is revoked
            CompletableFuture<Void> first = new CompletableFuture<>();
            handler = offset -> offset == 0 ? first : FutureUtils.emptyCompletedFuture();
            assignWithRecords(eventRecord(0), eventRecord(1));
            start(configuration());
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L)));

            // when
            consumer.schedulePollTask(() -> consumer.rebalance(List.of()));
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.assignment().isEmpty());
            first.complete(null);

            // then - the next owner processes the remaining record, so this member must not
            await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
                   .until(() -> processed.equals(List.of(0L)));
            assertThat(consumer.commits).isEmpty();
        }

        @Test
        void revokedPartitionCommitsTheRecordInProgressAndStartsNoFurtherRecords() {
            // given - the first record is in progress, and the second would never finish
            CompletableFuture<Void> first = new CompletableFuture<>();
            handler = offset -> offset == 0 ? first : new CompletableFuture<>();
            assignWithRecords(eventRecord(0), eventRecord(1));
            start(configuration().withRevocationTimeout(Duration.ofSeconds(5)));
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L)));

            // when - the first record finishes while Kafka takes the partition away
            consumer.schedulePollTask(() -> {
                CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS).execute(() -> first.complete(null));
                consumer.rebalance(List.of());
            });

            // then - the progress is committed, and the next owner continues with the second record
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.assignment().isEmpty());
            assertThat(consumer.commitRequests).containsExactly(Map.of(PARTITION, 1L));
            await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
                   .until(() -> processed.equals(List.of(0L)));
        }

        @Test
        void revokedPartitionsCommitTheEventsThatFinishWithinTheRevocationTimeoutInOneRequest() {
            // given - the records of both partitions are still being processed
            CompletableFuture<Void> processing = new CompletableFuture<>();
            handler = offset -> processing;
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(PARTITION, OTHER_PARTITION));
                consumer.addRecord(eventRecord(PARTITION, 0));
                consumer.addRecord(eventRecord(OTHER_PARTITION, 0));
            });
            start(configuration().withRevocationTimeout(Duration.ofSeconds(5)));
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.size() == 2);

            // when - the events finish while Kafka takes the partitions away
            consumer.schedulePollTask(() -> {
                processing.complete(null);
                consumer.rebalance(List.of());
            });

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.assignment().isEmpty());
            assertThat(consumer.commitRequests).containsExactly(Map.of(PARTITION, 1L, OTHER_PARTITION, 1L));
        }
    }

    @Nested
    class LostPartitions {

        @Test
        void lostPartitionIsNeitherCommittedNorProcessedFurther() {
            // given - the first record is still being processed when the partition is lost
            CompletableFuture<Void> first = new CompletableFuture<>();
            handler = offset -> offset == 0 ? first : FutureUtils.emptyCompletedFuture();
            assignWithRecords(eventRecord(0), eventRecord(1));
            start(configuration());
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L)));

            // when
            CompletableFuture<Void> lost = new CompletableFuture<>();
            consumer.schedulePollTask(() -> {
                consumer.listener().onPartitionsLost(List.of(PARTITION));
                lost.complete(null);
            });
            lost.orTimeout(5, TimeUnit.SECONDS).join();
            first.complete(null);

            // then - another member owns the partition already
            await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
                   .until(() -> processed.equals(List.of(0L)));
            assertThat(consumer.commits).isEmpty();
        }
    }

    @Nested
    class Stopping {

        @Test
        void commitsProgressWhenTheStopRequestWakesTheConsumerOutsideOfPoll() {
            // given
            CompletableFuture<Void> processing = new CompletableFuture<>();
            handler = offset -> processing;
            consumer.wakeUpOutsideOfPoll = true;
            assignWithRecords(eventRecord(0));
            start(configuration().withRevocationTimeout(Duration.ofSeconds(5)));
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L)));

            // when
            testSubject.requestStop();
            processing.complete(null);

            // then
            assertThat(testSubject.awaitStop(Duration.ofSeconds(10))).isTrue();
            assertThat(consumer.commits).containsExactly(1L);
            assertThat(consumer.closed()).isTrue();
        }

        @Test
        void waitsForUnfinishedEventsOnlyOnce() {
            // given - an event that never finishes, and a consumer that revokes its partitions when it is closed
            consumer.revokeOnClose = true;
            handler = offset -> new CompletableFuture<>();
            assignWithRecords(eventRecord(0), eventRecord(1));
            start(configuration().withRevocationTimeout(Duration.ofSeconds(3)));
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L)));
            long stopRequested = System.nanoTime();

            // when
            testSubject.requestStop();

            // then - one revocation timeout passes, not two, and the unfinished event is left for the next owner. The
            // margin between one wait (3 seconds) and two (6 seconds) leaves ample room for a slow build machine.
            assertThat(testSubject.awaitStop(Duration.ofSeconds(15))).isTrue();
            assertThat(Duration.ofNanos(System.nanoTime() - stopRequested)).isLessThan(Duration.ofMillis(5500));
            assertThat(consumer.commits).isEmpty();
            assertThat(processed).containsExactly(0L);
        }

        @Test
        void stopDuringADeadLetterWriteCommitsNoRecordThatWasNotProcessed() {
            // given - partition 0 processes a batch of five records, the second of which is slow, while the polling
            // thread waits for the dead-letter topic to acknowledge the failed record of partition 1
            MockProducer<String, byte[]> deadLetters =
                    new MockProducer<>(false, null, new StringSerializer(), new ByteArraySerializer());
            CompletableFuture<Void> slowRecord = new CompletableFuture<>();
            partitionHandler = (partition, offset) -> offset == 1 ? slowRecord : FutureUtils.emptyCompletedFuture();
            failingPartition = OTHER_PARTITION.partition();
            consumer.wakeUpOutsideOfPoll = true;
            assignBatchOfFiveRecordsAndOneRecordOfTheOtherPartition();
            start(configuration().withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC)
                                 .withRevocationTimeout(Duration.ofSeconds(5)), () -> consumer, () -> deadLetters);
            await().atMost(Duration.ofSeconds(5)).until(
                    () -> deadLetters.history().size() == 1 && processedOf(PARTITION).contains(1L)
            );
            addRecordsAfterTheBatch();

            // when - the slow record finishes after the stop request, before the dead-letter topic acknowledges
            testSubject.requestStop();
            slowRecord.complete(null);
            await().atMost(Duration.ofSeconds(5)).until(processingExecutor::idle);
            assertThat(deadLetters.completeNext()).isTrue();

            // then
            assertOnlyTheProcessedRecordsOfTheBatchAreCommitted();
            assertThat(consumer.commitsByPartition.get(OTHER_PARTITION)).containsExactly(1L);
        }

        @Test
        void stopWhileThePollingThreadHandlesCompletedBatchesCommitsNoRecordThatWasNotProcessed() {
            // given - partition 0 processes a batch of five records, the second of which is slow
            CompletableFuture<Void> slowRecord = new CompletableFuture<>();
            CompletableFuture<Void> otherRecord = new CompletableFuture<>();
            partitionHandler = slowSecondRecordAnd(slowRecord, otherRecord);
            consumer.wakeUpOutsideOfPoll = true;
            assignBatchOfFiveRecordsAndOneRecordOfTheOtherPartition();
            start(configuration().withRevocationTimeout(Duration.ofSeconds(5)));
            await().atMost(Duration.ofSeconds(5)).until(
                    () -> processedOf(PARTITION).contains(1L) && processedOf(OTHER_PARTITION).contains(0L)
            );
            addRecordsAfterTheBatch();
            // the polling thread is held while it handles the completed batch of partition 1
            CompletableFuture<Void> release = new CompletableFuture<>();
            consumer.holdPollingThreadInNextAssignmentCall(release);
            otherRecord.complete(null);
            consumer.assignmentGateReached.orTimeout(5, TimeUnit.SECONDS).join();

            // when - the stop request arrives after the polling thread decided to poll once more
            testSubject.requestStop();
            slowRecord.complete(null);
            await().atMost(Duration.ofSeconds(5)).until(processingExecutor::idle);
            release.complete(null);

            // then
            assertOnlyTheProcessedRecordsOfTheBatchAreCommitted();
        }

        @Test
        void stopDuringARebalanceCommitsNoRecordThatWasNotProcessedOfARetainedPartition() {
            // given - partition 0 processes a batch of five records, the second of which is slow
            CompletableFuture<Void> slowRecord = new CompletableFuture<>();
            CompletableFuture<Void> otherRecord = new CompletableFuture<>();
            partitionHandler = slowSecondRecordAnd(slowRecord, otherRecord);
            consumer.wakeUpOutsideOfPoll = true;
            assignBatchOfFiveRecordsAndOneRecordOfTheOtherPartition();
            start(configuration().withRevocationTimeout(Duration.ofSeconds(5)));
            await().atMost(Duration.ofSeconds(5)).until(
                    () -> processedOf(PARTITION).contains(1L) && processedOf(OTHER_PARTITION).contains(0L)
            );
            addRecordsAfterTheBatch();
            // a cooperative rebalance takes partition 1 away and leaves partition 0 with this member
            CompletableFuture<Void> revoking = new CompletableFuture<>();
            consumer.schedulePollTask(() -> {
                revoking.complete(null);
                consumer.listener().onPartitionsRevoked(List.of(OTHER_PARTITION));
            });
            revoking.orTimeout(5, TimeUnit.SECONDS).join();

            // when - the member is asked to stop while it waits for the record of the revoked partition
            testSubject.requestStop();
            slowRecord.complete(null);
            await().atMost(Duration.ofSeconds(5)).until(processingExecutor::idle);
            otherRecord.complete(null);

            // then
            assertOnlyTheProcessedRecordsOfTheBatchAreCommitted();
            assertThat(consumer.commitsByPartition.get(OTHER_PARTITION)).containsExactly(1L);
        }

        @Test
        void stopShortensTheWaitOfARevocationInProgress() {
            // given - events that never finish in two partitions, while Kafka takes one of them away
            handler = offset -> new CompletableFuture<>();
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(PARTITION, OTHER_PARTITION));
                consumer.addRecord(eventRecord(PARTITION, 0));
                consumer.addRecord(eventRecord(OTHER_PARTITION, 0));
            });
            start(configuration().withRevocationTimeout(Duration.ofSeconds(3)));
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.size() == 2);
            CompletableFuture<Void> revoking = new CompletableFuture<>();
            consumer.schedulePollTask(() -> {
                revoking.complete(null);
                consumer.listener().onPartitionsRevoked(List.of(OTHER_PARTITION));
            });
            revoking.orTimeout(5, TimeUnit.SECONDS).join();
            long stopRequested = System.nanoTime();

            // when
            testSubject.requestStop();

            // then - the revocation and the final wait for the retained partition together take one revocation
            // timeout from the stop request, not the revocation's own timeout plus another one. The margin between the
            // two (3 and 6 seconds) leaves ample room for a slow build machine.
            assertThat(testSubject.awaitStop(Duration.ofSeconds(15))).isTrue();
            assertThat(Duration.ofNanos(System.nanoTime() - stopRequested)).isLessThan(Duration.ofMillis(4500));
            assertThat(consumer.commits).isEmpty();
        }

        @Test
        void stopEndsTheWaitForADeadLetterAcknowledgementAtTheRevocationTimeout() {
            // given - a dead-letter topic that never acknowledges
            MockProducer<String, byte[]> deadLetters =
                    new MockProducer<>(false, null, new StringSerializer(), new ByteArraySerializer());
            handler = offset -> CompletableFuture.failedFuture(new IllegalStateException("failed"));
            assignWithRecords(eventRecord(0), eventRecord(1));
            start(configuration().withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC)
                                 .withRevocationTimeout(Duration.ofSeconds(1)), () -> consumer, () -> deadLetters);
            await().atMost(Duration.ofSeconds(5)).until(() -> deadLetters.history().size() == 1);
            long stopRequested = System.nanoTime();

            // when
            testSubject.requestStop();

            // then - the member stops well within the 20 seconds an acknowledgement may take, and leaves the failed
            // record for the partition's next owner
            assertThat(testSubject.awaitStop(Duration.ofSeconds(15))).isTrue();
            assertThat(Duration.ofNanos(System.nanoTime() - stopRequested)).isLessThan(Duration.ofSeconds(5));
            assertThat(consumer.commits).allMatch(offset -> offset == 0L);
        }

        private void assignBatchOfFiveRecordsAndOneRecordOfTheOtherPartition() {
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(PARTITION, OTHER_PARTITION));
                for (long offset = 0; offset < 5; offset++) {
                    consumer.addRecord(eventRecord(PARTITION, offset));
                }
                consumer.addRecord(eventRecord(OTHER_PARTITION, 0));
            });
        }

        /**
         * Adds records after the batch in progress, which the consumer returns once partition 0 is resumed. A real
         * consumer whose position is moved back returns the records from that position on; the mock consumer returns
         * these, whatever the position.
         */
        private void addRecordsAfterTheBatch() {
            for (long offset = 5; offset < 8; offset++) {
                consumer.addRecord(eventRecord(PARTITION, offset));
            }
        }

        private BiFunction<Integer, Long, CompletableFuture<?>> slowSecondRecordAnd(CompletableFuture<Void> slowRecord,
                                                                                  CompletableFuture<Void> otherRecord) {
            return (partition, offset) -> {
                if (partition == OTHER_PARTITION.partition()) {
                    return otherRecord;
                }
                return offset == 1 ? slowRecord : FutureUtils.emptyCompletedFuture();
            };
        }

        private void assertOnlyTheProcessedRecordsOfTheBatchAreCommitted() {
            assertThat(testSubject.awaitStop(Duration.ofSeconds(10))).isTrue();
            assertThat(processedOf(PARTITION)).containsExactly(0L, 1L);
            // the records after the slow one are left for the partition's next owner
            assertThat(consumer.commitsByPartition.get(PARTITION)).containsExactly(2L);
            // the position, which the batch had moved past its last record, is moved back to the first record that was
            // not processed, and records returned after the stop request are not dispatched, which would move it again
            assertThat(consumer.seeksByPartition.get(PARTITION)).endsWith(2L);
        }
    }

    @Nested
    class UndecodableRecords {

        @Test
        void skipsARecordThatCannotBeDeserialized() {
            // given
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(PARTITION));
                consumer.addRecord(eventRecord(1));
                consumer.setPollException(ConsumedRecords.undeserializable(PARTITION, 0));
            });

            // when
            start(configuration());

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(1L)));
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.equals(List.of(1L, 2L)));
        }

        @Test
        void stopsAtARecordThatCannotBeDeserializedUnderTheFailPolicy() {
            // given - the record after the one that cannot be deserialized is available to the consumer
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(PARTITION));
                consumer.addRecord(eventRecord(1));
                consumer.setPollException(ConsumedRecords.undeserializable(PARTITION, 0));
            });

            // when
            start(configuration().withUndecodableRecordPolicy(UndecodableRecordPolicy.FAIL));

            // then - consumption stops at the record, instead of retrying it
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.paused().contains(PARTITION));
            await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                   .until(() -> processed.isEmpty() && consumer.paused().contains(PARTITION));
            assertThat(consumer.commits).allMatch(offset -> offset <= 0L);
        }

        @Test
        void skipsARecordThatCannotBeDeserializedWhileItsPartitionIsInFlight() {
            // given - the record after the one being processed cannot be deserialized
            CompletableFuture<Void> first = new CompletableFuture<>();
            handler = offset -> offset == 0 ? first : FutureUtils.emptyCompletedFuture();
            assignWithRecords(eventRecord(0), eventRecord(2));
            consumer.schedulePollTask(() -> consumer.setPollException(ConsumedRecords.undeserializable(PARTITION, 1)));
            start(configuration());
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L)));

            // when
            first.complete(null);

            // then - consumption continues after the skipped record
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L, 2L)));
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(3L));
        }

        @Test
        void stopsAtARecordThatCannotBeDeserializedOnceItsPartitionIsNoLongerInFlight() {
            // given - the consumer raises the exception again after the records in progress are done, as a real
            // consumer does
            CompletableFuture<Void> first = new CompletableFuture<>();
            handler = offset -> {
                if (offset == 0) {
                    first.thenRun(() -> consumer.schedulePollTask(
                            () -> consumer.setPollException(ConsumedRecords.undeserializable(PARTITION, 1))
                    ));
                    return first;
                }
                return FutureUtils.emptyCompletedFuture();
            };
            assignWithRecords(eventRecord(0));
            consumer.schedulePollTask(() -> consumer.setPollException(ConsumedRecords.undeserializable(PARTITION, 1)));
            start(configuration().withUndecodableRecordPolicy(UndecodableRecordPolicy.FAIL));
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L)));

            // when
            first.complete(null);

            // then - the record in progress is committed, and the partition stops at the rejected record
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(1L));
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.paused().contains(PARTITION));
            assertThat(consumer.commits).allMatch(offset -> offset <= 1L);
        }

        @Test
        void skipsARecordWhoseContentCannotBeDecoded() {
            // given
            assignWithRecords(malformedRecord(0), eventRecord(1));

            // when
            start(configuration());

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(1L)));
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(2L));
        }

        @Test
        void skipsARecordWhoseDecodingThrowsAnyRuntimeException() {
            // given
            recordCodec = CodecTestFixtures.failingAt(codec, 0, new IllegalArgumentException("unknown event type"));
            assignWithRecords(eventRecord(0), eventRecord(1));

            // when
            start(configuration());

            // then - the record is skipped, rather than handed to the failed-record policy and retried
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(1L)));
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(2L));
        }

        @ParameterizedTest
        @EnumSource(FailedRecordPolicy.class)
        void stopsAtARecordWhoseContentCannotBeDecodedUnderTheFailPolicyWhateverTheFailedRecordPolicy(
                FailedRecordPolicy failedRecordPolicy
        ) {
            // given
            assignWithRecords(eventRecord(0), malformedRecord(1), eventRecord(2));

            // when
            start(configuration().withUndecodableRecordPolicy(UndecodableRecordPolicy.FAIL)
                                 .withFailedRecordPolicy(failedRecordPolicy));

            // then - the record before it is processed and committed; nothing moves past it
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(1L));
            await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                   .until(() -> processed.equals(List.of(0L)) && consumer.paused().contains(PARTITION));
            assertThat(consumer.commits).allMatch(offset -> offset <= 1L);
            assertThat(deadLetterProducer.history()).isEmpty();
        }

        @Test
        void stopsAtARecordWhoseDecodingThrowsAnyRuntimeExceptionUnderTheFailPolicy() {
            // given
            recordCodec = CodecTestFixtures.failingAt(codec, 0, new IllegalArgumentException("unknown event type"));
            assignWithRecords(eventRecord(0), eventRecord(1));

            // when
            start(configuration().withUndecodableRecordPolicy(UndecodableRecordPolicy.FAIL)
                                 .withFailedRecordPolicy(FailedRecordPolicy.SKIP));

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.paused().contains(PARTITION));
            await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                   .until(() -> processed.isEmpty() && consumer.paused().contains(PARTITION));
            assertThat(consumer.commits).allMatch(offset -> offset <= 0L);
        }
    }

    @Nested
    class FailedRecords {

        @ParameterizedTest
        @ValueSource(ints = {1, 3})
        void recordWhoseTaskTheExecutorRejectsIsRetriedWithoutApplyingTheFailedRecordPolicy(int rejectedTask) {
            // given - the executor rejects the task of one of four records: the first, or the third
            processingExecutor.rejectedTask = rejectedTask;
            long rejected = rejectedTask - 1;
            assignWithRecords(eventRecord(0), eventRecord(1), eventRecord(2), eventRecord(3));

            // when
            start(configuration().withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC));

            // then - the progress up to the rejected record is committed, and no processed record is dead-lettered
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(rejected));
            assertThat(processed).containsExactlyElementsOf(List.of(0L, 1L, 2L, 3L).subList(0, (int) rejected));

            // when - the member seeks back to the rejected record; the mock consumer needs the records again
            consumer.schedulePollTask(() -> {
                for (long offset = rejected; offset < 4; offset++) {
                    consumer.addRecord(eventRecord(offset));
                }
            });

            // then - the rejected record and the ones after it are processed once
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(4L));
            assertThat(processed).containsExactly(0L, 1L, 2L, 3L);
            assertThat(consumer.commits).containsExactly(rejected, 4L);
            assertThat(deadLetterProducer.history()).isEmpty();
        }

        @Test
        void retriedRecordIsClaimedOnce() {
            // given
            handler = offset -> {
                if (attempts.get(offset).get() > 1) {
                    return FutureUtils.emptyCompletedFuture();
                }
                // the member seeks back to the failed record; the mock consumer needs it again to redeliver it
                consumer.schedulePollTask(() -> consumer.addRecord(eventRecord(offset)));
                return CompletableFuture.failedFuture(new IllegalStateException("first attempt fails"));
            };
            assignWithRecords(eventRecord(0));

            // when
            start(configuration());

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(1L));
            assertThat(processed).containsExactly(0L, 0L);
            assertThat(entitlementManager.claims()).hasSize(1);
        }

        @Test
        void recordIsRetriedWhenTheDeadLetterProducerIsRefused() {
            // given - the source refuses dead-letter producers once it is stopping
            handler = offset -> {
                if (attempts.get(offset).get() > 1) {
                    return FutureUtils.emptyCompletedFuture();
                }
                consumer.schedulePollTask(() -> consumer.addRecord(eventRecord(offset)));
                return CompletableFuture.failedFuture(new IllegalStateException("first attempt fails"));
            };
            assignWithRecords(eventRecord(0));
            Supplier<Producer<String, byte[]>> refused = () -> {
                throw new IllegalStateException("The Kafka event source is stopping.");
            };

            // when
            start(configuration().withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC), () -> consumer,
                  refused);

            // then - the record is not skipped: it is processed again, and only then committed
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(1L));
            assertThat(processed).containsExactly(0L, 0L);
            assertThat(consumer.commits).allMatch(offset -> offset <= 1L);
        }

        @Test
        void recordIsRetriedWithoutCommittingPastItWhenSendingToTheDeadLetterTopicFails() {
            // given - a dead-letter producer that only completes sends when told to, and a handler that fails once
            MockProducer<String, byte[]> deadLetters =
                    new MockProducer<>(false, null, new StringSerializer(), new ByteArraySerializer());
            CompletableFuture<Void> retry = new CompletableFuture<>();
            handler = offset -> {
                if (attempts.get(offset).get() > 1) {
                    return retry;
                }
                consumer.schedulePollTask(() -> consumer.addRecord(eventRecord(offset)));
                return CompletableFuture.failedFuture(new IllegalStateException("first attempt fails"));
            };
            assignWithRecords(eventRecord(0));
            start(configuration().withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC), () -> consumer,
                  () -> deadLetters);
            await().atMost(Duration.ofSeconds(5)).until(() -> deadLetters.history().size() == 1);

            // when
            assertThat(deadLetters.errorNext(new KafkaException("dead-letter topic unavailable"))).isTrue();

            // then - the record is processed again, and nothing is committed past it in the meantime
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.equals(List.of(0L, 0L)));
            assertThat(consumer.commits).allMatch(offset -> offset <= 0L);

            // when
            retry.complete(null);

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(1L));
            assertThat(deadLetters.history()).hasSize(1);
            assertThat(processed).containsExactly(0L, 0L);
        }

        @Test
        void writesAtMostOneDeadLetterRecordBetweenTwoPolls() {
            // given - the records of two partitions fail at the same time
            List<Integer> pollsAtSend = new CopyOnWriteArrayList<>();
            MockProducer<String, byte[]> deadLetters =
                    new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer()) {
                        @Override
                        public synchronized Future<RecordMetadata> send(ProducerRecord<String, byte[]> record,
                                                                        Callback callback) {
                            pollsAtSend.add(consumer.polls.get());
                            return super.send(record, callback);
                        }
                    };
            CompletableFuture<Void> failing = new CompletableFuture<>();
            // wake-ups from other threads must not wait for the consumer's lock, which the held poll keeps
            consumer.wakeUpOutsideOfPoll = true;
            handler = offset -> failing.thenCompose(
                    done -> CompletableFuture.failedFuture(new IllegalStateException("failed"))
            );
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(PARTITION, OTHER_PARTITION));
                consumer.addRecord(eventRecord(PARTITION, 0));
                consumer.addRecord(eventRecord(OTHER_PARTITION, 0));
            });
            start(configuration().withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC), () -> consumer,
                  () -> deadLetters);
            await().atMost(Duration.ofSeconds(5)).until(() -> processed.size() == 2);

            // when - both batches complete while the polling thread is held in poll, before it handles them
            consumer.schedulePollTask(() -> {
                failing.complete(null);
                await().atMost(Duration.ofSeconds(5)).until(processingExecutor::idle);
            });

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> pollsAtSend.size() == 2);
            assertThat(pollsAtSend.get(1)).isGreaterThan(pollsAtSend.get(0));
            await().atMost(Duration.ofSeconds(5)).until(
                    () -> consumer.commitsByPartition.getOrDefault(PARTITION, List.of()).contains(1L)
                            && consumer.commitsByPartition.getOrDefault(OTHER_PARTITION, List.of()).contains(1L)
            );
        }

        @Test
        void deadLetterRecordKeepsTheTimestampOfTheOriginalRecord() {
            // given
            handler = offset -> CompletableFuture.failedFuture(new IllegalStateException("failed"));
            assignWithRecords(eventRecordAt(0, 1_234L));

            // when
            start(configuration().withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC));

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(1L));
            assertThat(deadLetterProducer.history()).singleElement()
                                                    .extracting(ProducerRecord::timestamp)
                                                    .isEqualTo(1_234L);
        }

        @Test
        void deadLetterRecordOfARecordWithoutTimestampGetsTheTimeOfWriting() {
            // given
            handler = offset -> CompletableFuture.failedFuture(new IllegalStateException("failed"));
            assignWithRecords(eventRecordAt(0, ConsumerRecord.NO_TIMESTAMP));

            // when
            start(configuration().withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC));

            // then - a record without timestamp leaves it to the producer to set one
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(1L));
            assertThat(deadLetterProducer.history()).singleElement()
                                                    .extracting(ProducerRecord::timestamp)
                                                    .isNull();
        }

        @Test
        void deadLetterRecordCarriesTheOriginalRecordAndItsPosition() {
            // given
            handler = offset -> CompletableFuture.failedFuture(new IllegalStateException("failed"));
            ConsumerRecord<String, byte[]> original = eventRecord(0);
            ConsumerRecord<String, byte[]> keyed = new ConsumerRecord<>(
                    TOPIC, PARTITION.partition(), 0, original.timestamp(), TimestampType.CREATE_TIME, -1, -1,
                    "parcel-key", original.value(), original.headers(), Optional.empty()
            );
            assignWithRecords(keyed);

            // when
            start(configuration().withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC));

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(1L));
            assertThat(deadLetterProducer.history()).singleElement().satisfies(deadLetter -> {
                assertThat(deadLetter.key()).isEqualTo("parcel-key");
                assertThat(deadLetter.value()).isEqualTo(original.value());
                assertThat(header(deadLetter.headers(), DefaultKafkaEventCodec.TYPE_HEADER))
                        .isEqualTo(header(original.headers(), DefaultKafkaEventCodec.TYPE_HEADER));
                assertThat(header(deadLetter.headers(), DeadLetterHeaders.ORIGINAL_TOPIC)).isEqualTo(TOPIC);
                assertThat(header(deadLetter.headers(), DeadLetterHeaders.ORIGINAL_PARTITION)).isEqualTo("0");
                assertThat(header(deadLetter.headers(), DeadLetterHeaders.ORIGINAL_OFFSET)).isEqualTo("0");
                assertThat(header(deadLetter.headers(), DeadLetterHeaders.EXCEPTION_MESSAGE))
                        .isEqualTo("failed");
            });
        }

        @Test
        void deadLetterRecordOmitsAMissingExceptionMessage() {
            // given
            handler = offset -> CompletableFuture.failedFuture(new IllegalStateException());
            assignWithRecords(eventRecord(0));

            // when
            start(configuration().withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC));

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.commits.contains(1L));
            assertThat(deadLetterProducer.history()).singleElement().satisfies(deadLetter -> {
                assertThat(deadLetter.topic()).isEqualTo(TOPIC + ".DLT");
                assertThat(deadLetter.headers().lastHeader(DeadLetterHeaders.EXCEPTION_CLASS)).isNotNull();
                assertThat(deadLetter.headers().lastHeader(DeadLetterHeaders.EXCEPTION_MESSAGE)).isNull();
            });
        }
    }
}
