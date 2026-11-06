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

import io.axoniq.framework.kafka.codec.DecodedRecord;
import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import io.axoniq.framework.kafka.codec.UndecodableRecordException;
import io.axoniq.framework.kafka.common.KafkaAxoniqAddon;
import io.axoniq.framework.kafka.common.KafkaConnectorUtils;
import io.axoniq.framework.kafka.common.KafkaResources;
import io.axoniq.framework.kafka.common.RetentionGapPolicy;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.license.entitlement.EntitlementMessageType;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
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
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * One member of a consumer group: a thread that polls a Kafka consumer and hands the records of each partition to the
 * subscriber, in order per partition and concurrently across partitions.
 * <p>
 * Kafka consumers are not thread-safe, so every call on the consumer happens on the polling thread: polling, pausing
 * and resuming partitions, seeking and committing offsets. While a partition's records are processed, the partition
 * is paused and the thread keeps polling, so that slow handlers never exceed {@code max.poll.interval.ms}. Once the
 * records are processed, the thread commits the offset after the last processed record and resumes the partition.
 * <p>
 * Every event is processed in a unit of work of its own, whose context carries the
 * {@link KafkaResources} of its record and the resources the codec decoded. A failed event is dealt with according
 * to the {@link FailedRecordPolicy}, and a record that yields no event according to the
 * {@link io.axoniq.framework.kafka.codec.UndecodableRecordPolicy}. When that policy rejects a record, the member
 * stops consuming the record's partition at that record, whatever the {@link FailedRecordPolicy}. A record whose
 * processing cannot start, because the executor rejects its task, is retried after a backoff, without applying the
 * {@link FailedRecordPolicy}.
 * <p>
 * A failed offset commit is not fatal: the records it covers are delivered again, to this member or to the next owner
 * of the partition. When a partition is revoked while its records are processed, processing stops after the record in
 * progress, waiting at most the revocation timeout for it, and the progress is committed, so that the partition's next
 * owner continues right after it. A failure that retrying cannot resolve, such as invalid settings or failed
 * authentication or authorization, stops the member: it logs the failure once and completes its
 * {@link #failure() failure future} exceptionally. So does an {@link Error} on the polling thread. Any other failure,
 * including bootstrap servers that do not resolve (yet), ends the member's consumer; the member then creates a new
 * consumer and rejoins the group, after a backoff that grows with consecutive failures.
 * <p>
 * A partition without a committed offset, of a new consumer group or a new partition, is consumed from the configured
 * {@link InitialPosition}: its earliest record, by default, or its end. A committed offset that Kafka has already
 * deleted under the topic's retention settings is dealt with according to the {@link RetentionGapPolicy}. Both rely
 * on the consumer's {@code auto.offset.reset} setting being {@code none}, which the owning source sets unless
 * configured otherwise.
 * <p>
 * The polling thread must call {@code poll} at least every {@code max.poll.interval.ms} (5 minutes by default), or the
 * consumer leaves the group. Besides waiting for records, it may block while it writes a record to a dead-letter
 * topic, one record between two polls, for at most the dead-letter producer's {@code max.block.ms} plus
 * {@link ShutdownBudget#DEAD_LETTER_ACK_TIMEOUT} for the acknowledgement, and, inside {@code poll}, while Kafka takes
 * partitions away, for at most the revocation timeout plus the time to commit. Keep these well below
 * {@code max.poll.interval.ms}.
 * <p>
 * Internal, because it is created by and only reachable through {@link KafkaSubscribableEventSource}.
 *
 * @param <V> the type of record value
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
class GroupConsumer<V> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(GroupConsumer.class);
    private final Supplier<Consumer<String, V>> consumerFactory;
    private final KafkaSubscribableEventSourceConfiguration configuration;
    private final KafkaEventCodec<V> codec;
    private final BiFunction<List<? extends EventMessage>, @Nullable ProcessingContext, CompletableFuture<?>>
            subscriber;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Executor executor;
    private final DeadLetterTopicWriter<V> deadLetterTopicWriter;
    private final EntitlementManager entitlementManager;

    private final Map<TopicPartition, Batch> inFlight = new HashMap<>();
    private final Queue<BatchResult> completions = new ConcurrentLinkedQueue<>();
    private final Map<TopicPartition, Long> resumeAt = new HashMap<>();
    private final Map<TopicPartition, Integer> attempts = new HashMap<>();
    private final Map<TopicPartition, Long> claimedUpTo = new ConcurrentHashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> pendingCommits = new HashMap<>();
    private final Map<TopicPartition, StoppedPartition> stoppedPartitions = new ConcurrentHashMap<>();
    private final AtomicBoolean wakeUpOnCompletion = new AtomicBoolean();
    private final CompletableFuture<Void> failure = new CompletableFuture<>();
    private final StopSignal stop = new StopSignal();
    private volatile boolean running = true;
    private volatile @Nullable Consumer<String, V> consumer;
    private volatile @Nullable Thread thread;

    /**
     * The records of one partition that were fetched together, while they are processed.
     */
    private static final class Batch {

        private volatile boolean cancelled;
        private @Nullable CompletableFuture<BatchResult> result;
    }

    /**
     * How processing the records of one partition that were fetched together ended.
     */
    private enum Outcome {

        /**
         * The records were processed: all of them, or those up to the record in progress when the member was asked to
         * stop or the partition was revoked.
         */
        PROCESSED,
        /**
         * Processing the event of a record failed; the {@link FailedRecordPolicy} applies to the record.
         */
        FAILED,
        /**
         * The undecodable-record policy rejected a record.
         */
        UNDECODABLE,
        /**
         * Processing a record could not start, for example because the executor rejected its task. The record is
         * retried after the backoff, without applying the {@link FailedRecordPolicy}: its event never ran.
         */
        NOT_STARTED
    }

    /**
     * The outcome of processing the records of one partition that were fetched together.
     *
     * @param batch     the batch
     * @param partition the partition the records belong to
     * @param records   the records of the batch
     * @param index     the index of the record at which processing ended: the number of records processed
     *                  successfully, which equals the number of records when all were
     * @param outcome   how processing ended
     * @param failure   the failure of the record at {@code index}, or {@code null} when processing did not fail
     */
    private record BatchResult(Batch batch,
                               TopicPartition partition,
                               List<? extends ConsumerRecord<String, ?>> records,
                               int index,
                               Outcome outcome,
                               @Nullable Throwable failure) {

        /**
         * Returns whether all records of the batch were processed.
         */
        boolean complete() {
            return index == records.size();
        }

        /**
         * Returns the offset to continue from: that of the record at which processing ended, or the offset after the
         * last record when all were processed. Every record before it was processed successfully.
         */
        long nextOffset() {
            return complete() ? records.getLast().offset() + 1 : records.get(index).offset();
        }

        /**
         * Returns the record at which processing ended, which failed or did not start.
         */
        ConsumerRecord<String, ?> record() {
            return records.get(index);
        }
    }

    /**
     * Constructs a group member polling with consumers from the given {@code consumerFactory}. The member owns and
     * closes those consumers, and asks for a new one only after a failure ended the previous one.
     *
     * @param consumerFactory       creates the consumers, configured with the group identifier, not subscribed yet
     * @param configuration         the settings of the owning source
     * @param codec                 the codec that decodes records
     * @param subscriber            the subscriber that processes the events
     * @param unitOfWorkFactory     the factory of the unit of work each event is processed in
     * @param executor              the executor that processes records, off the polling thread
     * @param deadLetterTopicWriter writes failed records to dead-letter topics
     * @param entitlementManager    the entitlement manager processed events are claimed against
     */
    GroupConsumer(Supplier<Consumer<String, V>> consumerFactory,
                  KafkaSubscribableEventSourceConfiguration configuration,
                  KafkaEventCodec<V> codec,
                  BiFunction<List<? extends EventMessage>, @Nullable ProcessingContext, CompletableFuture<?>>
                          subscriber,
                  UnitOfWorkFactory unitOfWorkFactory,
                  Executor executor,
                  DeadLetterTopicWriter<V> deadLetterTopicWriter,
                  EntitlementManager entitlementManager) {
        this.consumerFactory = consumerFactory;
        this.configuration = configuration;
        this.codec = codec;
        this.subscriber = subscriber;
        this.unitOfWorkFactory = unitOfWorkFactory;
        this.executor = executor;
        this.deadLetterTopicWriter = deadLetterTopicWriter;
        this.entitlementManager = entitlementManager;
    }

    /**
     * Starts the polling thread.
     *
     * @param name the name of the thread
     */
    void start(String name) {
        thread = Thread.ofPlatform().name(name).daemon(true).start(this);
    }

    /**
     * Asks the polling thread to stop, without waiting for it. The thread finishes the events in progress, waiting at
     * most the configured revocation timeout from now, also when it was already waiting for them because Kafka takes
     * partitions away, commits their offsets and leaves the consumer group. Events that did not finish in time are
     * left for the partitions' next owners, and so are failed records that were not written to a dead-letter topic
     * yet. Stopping takes at most {@link ShutdownBudget#stopTimeout(Duration)}.
     */
    void requestStop() {
        stop.request(configuration.revocationTimeout());
        running = false;
        Consumer<String, V> current = consumer;
        if (current != null) {
            current.wakeup();
        }
        Thread pollingThread = thread;
        if (pollingThread != null) {
            LockSupport.unpark(pollingThread);
        }
    }

    /**
     * Returns a future that completes exceptionally, with the failure, when this member stops because of a failure
     * that retrying cannot resolve, such as invalid settings or failed authentication or authorization. It completes
     * normally when the member stops for any other reason.
     *
     * @return the future reporting why this member stopped
     */
    CompletableFuture<Void> failure() {
        return failure;
    }

    /**
     * Returns the partitions this member owns, but does not progress on: those it stopped at a record that the
     * undecodable-record policy rejected or at a retention gap, and those whose record it retries.
     *
     * @return the partitions this member does not progress on
     */
    Collection<StoppedPartition> stoppedPartitions() {
        return List.copyOf(stoppedPartitions.values());
    }

    /**
     * Waits until the polling thread has stopped, or the given {@code timeout} expires.
     *
     * @param timeout the longest time to wait
     * @return {@code true} if the thread has stopped
     */
    boolean awaitStop(Duration timeout) {
        Thread pollingThread = thread;
        if (pollingThread == null || pollingThread == Thread.currentThread()) {
            return true;
        }
        try {
            return pollingThread.join(timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void run() {
        try {
            consumeUntilStopped();
            failure.complete(null);
        } catch (Throwable e) {
            // An Error, such as a class missing at runtime, ends the polling thread; reported as a failure, so that the
            // source stops its other consumers and reports it, rather than appearing to run while nothing consumes.
            running = false;
            logger.error("Consuming events from Kafka in consumer group [{}] failed unexpectedly. The consumer stopped "
                                 + "and does not rejoin the group.", configuration.groupId(), e);
            failure.completeExceptionally(e);
            if (e instanceof VirtualMachineError error) {
                throw error;
            }
        }
    }

    private void consumeUntilStopped() {
        int failures = 0;
        while (running) {
            long sessionStart = System.nanoTime();
            Exception error = null;
            try {
                Consumer<String, V> session = consumerFactory.get();
                consumer = session;
                try {
                    consume(session);
                } finally {
                    KafkaConnectorUtils.closeQuietly(session, ShutdownBudget.CONSUMER_CLOSE_TIMEOUT);
                }
            } catch (Exception e) {
                error = e;
            }
            if (error == null) {
                continue;
            }
            if (!running) {
                logger.debug("Consuming events from Kafka in consumer group [{}] failed while stopping.",
                             configuration.groupId(), error);
                break;
            }
            if (ConsumerFailures.isPermanent(error)) {
                running = false;
                logger.error("Consuming events from Kafka in consumer group [{}] failed in a way that retrying cannot "
                                     + "resolve. The consumer stopped and does not rejoin the group.",
                             configuration.groupId(), error);
                failure.completeExceptionally(error);
                break;
            }
            // A consumer that ran for a while before failing starts the backoff afresh.
            failures = System.nanoTime() - sessionStart > configuration.retryMaxBackoff().toNanos()
                    ? 1 : failures + 1;
            long delay = backoff(failures);
            if (ConsumerFailures.isFenced(error)) {
                logger.error("Consuming events from Kafka in consumer group [{}] failed: another consumer joined the "
                                     + "group with the same group.instance.id. Every consumer, in every application "
                                     + "instance, needs a group.instance.id of its own; instances configured with the "
                                     + "same one fence each other off indefinitely. The consumer rejoins the group in "
                                     + "{} ms.", configuration.groupId(), delay, error);
            } else {
                logger.error("Consuming events from Kafka in consumer group [{}] failed. The consumer rejoins the "
                                     + "group in {} ms.", configuration.groupId(), delay, error);
            }
            forget(Set.copyOf(inFlight.keySet()));
            completions.clear();
            resumeAt.clear();
            attempts.clear();
            pendingCommits.clear();
            stoppedPartitions.clear();
            pause(delay);
        }
    }

    private void consume(Consumer<String, V> session) {
        subscribe(session,
                  excludingDeadLetterTopics(configuration.subscription(), configuration.deadLetterTopicSuffix()),
                  new RebalanceListener());
        while (running) {
            processCompletions(1);
            resumeDuePartitions();
            if (!running) {
                // A stop request whose wake-up a commit consumed would otherwise leave the poll waiting its full
                // timeout.
                break;
            }
            ConsumerRecords<String, V> records;
            // A batch that completes while the thread waits in poll wakes it up, so that its offsets are committed and
            // its partition resumed without waiting for the poll timeout.
            wakeUpOnCompletion.set(true);
            try {
                records = session.poll(completions.isEmpty() ? configuration.pollTimeout() : Duration.ZERO);
            } catch (WakeupException e) {
                // Woken up to stop, or because a batch completed; the loop condition decides.
                continue;
            } catch (RecordDeserializationException e) {
                handleUndeserializableRecord(e);
                continue;
            } catch (NoOffsetForPartitionException e) {
                // No committed offset yet, for a new consumer group or a new partition.
                if (configuration.initialPosition() == InitialPosition.LATEST) {
                    session.seekToEnd(e.partitions());
                    pinPositions(session, e.partitions());
                } else {
                    session.seekToBeginning(e.partitions());
                }
                continue;
            } catch (OffsetOutOfRangeException e) {
                handleRetentionGap(e);
                continue;
            } finally {
                wakeUpOnCompletion.set(false);
            }
            if (!running) {
                // Records of a poll that returns after the stop request are left for the partitions' next owners; they
                // continue from the committed offsets, which never pass these records.
                break;
            }
            for (TopicPartition partition : records.partitions()) {
                dispatch(partition, records.records(partition));
            }
        }
        awaitInFlight(Set.copyOf(inFlight.keySet()));
        processCompletions(0);
        // Events that did not finish in time are redelivered to the partitions' next owners. Cancelling their batches
        // keeps the revocation that closing the consumer triggers from waiting for them a second time.
        forget(Set.copyOf(inFlight.keySet()));
    }

    /**
     * Commits the current position of the given {@code partitions}, which were just moved to their end, so that a
     * rebalance or restart before the first record is processed continues from there, rather than from the end of the
     * partitions at that later time, which would skip the records published in between.
     */
    private void pinPositions(Consumer<String, V> session, Collection<TopicPartition> partitions) {
        // Runs outside of poll, where a completed batch must not wake the consumer up.
        wakeUpOnCompletion.set(false);
        try {
            for (TopicPartition partition : partitions) {
                long position;
                try {
                    position = session.position(partition, ShutdownBudget.COMMIT_TIMEOUT);
                } catch (WakeupException e) {
                    // A stop request, or a batch that completed during poll, left a wake-up pending.
                    position = session.position(partition, ShutdownBudget.COMMIT_TIMEOUT);
                }
                commit(partition, position);
            }
        } catch (KafkaException e) {
            pendingCommits.keySet().removeAll(partitions);
            logger.warn("Committing the end of the new partitions {} in consumer group [{}] failed. Until a record is "
                                + "processed, a rebalance moves them to their end again.",
                        partitions, configuration.groupId(), e);
            return;
        }
        flushCommits();
    }

    private static void subscribe(Consumer<?, ?> session,
                                  TopicSubscription subscription,
                                  ConsumerRebalanceListener listener) {
        switch (subscription) {
            case TopicSubscription.Topics(Set<String> names) -> session.subscribe(names, listener);
            case TopicSubscription.TopicPattern(Pattern pattern) -> session.subscribe(pattern, listener);
        }
    }

    /**
     * Returns the given {@code subscription}, without the topics whose names end in the given dead-letter
     * {@code suffix} when it is a pattern. Otherwise, the source would consume its own dead-letter topics, handling
     * failed events again, and dead-letter them in turn to ever longer-named topics.
     */
    static TopicSubscription excludingDeadLetterTopics(TopicSubscription subscription, String suffix) {
        if (subscription instanceof TopicSubscription.TopicPattern(Pattern pattern)) {
            return TopicSubscription.pattern(Pattern.compile(
                    "(?!.*" + Pattern.quote(suffix) + "$)(?:" + pattern.pattern() + ")", pattern.flags()
            ));
        }
        return subscription;
    }

    private void pause(long millis) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        long remaining;
        while (running && (remaining = deadline - System.nanoTime()) > 0) {
            LockSupport.parkNanos(remaining);
            if (Thread.interrupted()) {
                // Nothing interrupts the polling thread but a request to stop; an interrupt left set would also make
                // parkNanos return at once, spinning for the rest of the backoff.
                logger.debug("The consumer of consumer group [{}] was interrupted; it stops.", configuration.groupId());
                running = false;
            }
        }
    }

    private Consumer<String, V> consumer() {
        return Objects.requireNonNull(consumer);
    }

    private void dispatch(TopicPartition partition, List<ConsumerRecord<String, V>> records) {
        consumer().pause(Set.of(partition));
        Batch batch = new Batch();
        inFlight.put(partition, batch);
        // The result is queued before the future that the polling thread may wait for completes, so that a batch that
        // finishes within the revocation timeout is always committed.
        batch.result = processAsync(batch, partition, records, 0)
                .handle((r, e) -> {
                    // Processing reports its failures as results; anything else is unexpected, and the batch is
                    // processed again from its first record, without applying the failed-record policy.
                    BatchResult outcome = r != null ? r : new BatchResult(batch, partition, records, 0,
                                                                          Outcome.NOT_STARTED, e);
                    completions.add(outcome);
                    return outcome;
                });
        batch.result.thenRun(this::wakeUpPollingThread);
    }

    /**
     * Wakes up the polling thread when it waits in {@code poll}, at most once per poll.
     */
    private void wakeUpPollingThread() {
        Consumer<String, V> current = consumer;
        if (current != null && wakeUpOnCompletion.compareAndSet(true, false)) {
            current.wakeup();
        }
    }

    /**
     * Processes the records of a batch from the given {@code index} on, starting on the executor: processing never runs
     * on the polling thread, nor on the thread that completed the previous record, even when the executor is quick to
     * start. A task the executor rejects is reported as a record that did not start, rather than as a failure of the
     * previous record, which was processed.
     */
    private CompletableFuture<BatchResult> processAsync(Batch batch,
                                                        TopicPartition partition,
                                                        List<ConsumerRecord<String, V>> records,
                                                        int index) {
        if (endsAt(batch, records, index)) {
            return CompletableFuture.completedFuture(
                    new BatchResult(batch, partition, records, index, Outcome.PROCESSED, null)
            );
        }
        try {
            return CompletableFuture.supplyAsync(() -> processFrom(batch, partition, records, index), executor)
                                    .thenCompose(Function.identity());
        } catch (RejectedExecutionException e) {
            return CompletableFuture.completedFuture(
                    new BatchResult(batch, partition, records, index, Outcome.NOT_STARTED, e)
            );
        }
    }

    /**
     * Returns whether processing the batch ends before the record at the given {@code index}: after its last record,
     * or, after the record in progress, when the member is asked to stop or the partition is revoked.
     */
    private boolean endsAt(Batch batch, List<ConsumerRecord<String, V>> records, int index) {
        return index == records.size() || !running || batch.cancelled;
    }

    private CompletableFuture<BatchResult> processFrom(Batch batch,
                                                       TopicPartition partition,
                                                       List<ConsumerRecord<String, V>> records,
                                                       int index) {
        if (endsAt(batch, records, index)) {
            return CompletableFuture.completedFuture(
                    new BatchResult(batch, partition, records, index, Outcome.PROCESSED, null)
            );
        }
        ConsumerRecord<String, V> record = records.get(index);
        Optional<DecodedRecord> decoded;
        try {
            decoded = configuration.undecodableRecordPolicy().decode(codec, record);
        } catch (UndecodableRecordException rejected) {
            return CompletableFuture.completedFuture(
                    new BatchResult(batch, partition, records, index, Outcome.UNDECODABLE, rejected)
            );
        }
        CompletableFuture<?> processed;
        try {
            processed = decoded.isPresent() ? process(record, decoded.get()) : FutureUtils.emptyCompletedFuture();
        } catch (Exception e) {
            processed = CompletableFuture.failedFuture(e);
        }
        return processed.handle((ok, error) -> error)
                        .thenCompose(error -> error == null
                                ? processAsync(batch, partition, records, index + 1)
                                : CompletableFuture.completedFuture(
                                        new BatchResult(batch, partition, records, index, Outcome.FAILED, error)
                                ));
    }

    private CompletableFuture<?> process(ConsumerRecord<String, V> record, DecodedRecord decoded) {
        Context resources = KafkaResources.addToContext(decoded.resources(), record);
        EventMessage event = decoded.event();
        claimOnce(record);
        return unitOfWorkFactory.create().executeWithResult(context -> {
            ProcessingContext eventContext = context;
            for (Map.Entry<ResourceKey<?>, Object> resource : resources.resources().entrySet()) {
                eventContext = withResource(eventContext, resource.getKey(), resource.getValue());
            }
            return subscriber.apply(List.of(event), eventContext);
        });
    }

    private void claimOnce(ConsumerRecord<String, V> record) {
        // A record that is retried has been claimed when it was first processed.
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        Long previous = claimedUpTo.get(partition);
        if (previous == null || record.offset() > previous) {
            claimedUpTo.put(partition, record.offset());
            entitlementManager.claimMessage(KafkaAxoniqAddon.IDENTIFIER, EntitlementMessageType.EVENT, 1);
        }
    }

    @SuppressWarnings("unchecked")
    private static ProcessingContext withResource(ProcessingContext context, ResourceKey<?> key, Object value) {
        return context.withResource((ResourceKey<Object>) key, value);
    }

    /**
     * Handles the batches that completed since the last call: commits their progress, resumes their partitions, and
     * applies the policies to their failed records.
     * <p>
     * Writing a record to a dead-letter topic blocks the polling thread, so at most {@code deadLetterSends} records are
     * written per call, and none once the member is asked to stop. The progress before the other failed records is
     * committed, and they stay queued for the next calls; while completions are queued, the polling thread polls
     * without waiting.
     * <p>
     * Every offset committed here, and in the methods it calls, keeps one invariant: <b>a committed offset never passes
     * a record that was not processed successfully, skipped or written to a dead-letter topic</b>, and a partition is
     * resumed only at such an offset, so that no later poll and commit can pass such a record either.
     *
     * @param deadLetterSends the number of records this call may write to dead-letter topics
     */
    private void processCompletions(int deadLetterSends) {
        List<BatchResult> deferred = new ArrayList<>();
        int sendsLeft = deadLetterSends;
        BatchResult result;
        while ((result = completions.poll()) != null) {
            Batch current = inFlight.get(result.partition());
            if (current != result.batch()) {
                // The partition was revoked in the meantime; its new owner processes these records again.
                continue;
            }
            if (needsDeadLetterTopic(result)) {
                if (sendsLeft == 0 || !running) {
                    // Keeps the progress of the records before the failed one, should the partition move elsewhere.
                    commit(result.partition(), result.nextOffset());
                    deferred.add(result);
                    continue;
                }
                sendsLeft--;
            }
            inFlight.remove(result.partition());
            switch (result.outcome()) {
                case PROCESSED -> handleProcessed(result);
                case FAILED -> handleFailure(result);
                case UNDECODABLE ->
                        stopAt(result.partition(), result.nextOffset(), Objects.requireNonNull(result.failure()));
                case NOT_STARTED -> retry(result, FutureUtils.unwrap(Objects.requireNonNull(result.failure())),
                                          "could not start, for example because the executor rejected it");
            }
        }
        completions.addAll(deferred);
        flushCommits();
    }

    private boolean needsDeadLetterTopic(BatchResult result) {
        return result.outcome() == Outcome.FAILED
                && configuration.failedRecordPolicy() == FailedRecordPolicy.DEAD_LETTER_TOPIC;
    }

    private void handleProcessed(BatchResult result) {
        TopicPartition partition = result.partition();
        if (result.index() > 0) {
            attempts.remove(partition);
            stoppedPartitions.remove(partition);
            commit(partition, result.nextOffset());
        }
        if (!result.complete() && owns(partition)) {
            // Processing ended early, as the member stops or the partition is revoked, while the consumer's position
            // is past the whole batch already. Moving it back keeps a later poll from returning, and a later commit
            // from passing, the records that were not processed.
            consumer().seek(partition, result.nextOffset());
        }
        resume(partition);
    }

    private void handleFailure(BatchResult result) {
        TopicPartition partition = result.partition();
        ConsumerRecord<String, ?> record = result.record();
        Throwable failure = FutureUtils.unwrap(Objects.requireNonNull(result.failure()));
        FailedRecordPolicy policy = configuration.failedRecordPolicy();
        if (policy == FailedRecordPolicy.DEAD_LETTER_TOPIC && deadLetterTopicWriter.write(record, failure, stop)) {
            continueAfter(partition, record.offset());
        } else if (policy == FailedRecordPolicy.SKIP) {
            logger.warn("Skipping the record at offset [{}] of partition [{}]: processing its event failed.",
                        record.offset(), partition, failure);
            continueAfter(partition, record.offset());
        } else {
            retry(result, failure, "failed");
        }
    }

    /**
     * Retries the record at which processing ended, after the backoff. A record whose processing could not start, for
     * example because the executor rejected its task, is retried this way too, without applying the
     * {@link FailedRecordPolicy}: its event never ran.
     *
     * @param result  the outcome of the batch whose record is retried
     * @param failure the failure of the record
     * @param reason  how processing the record ended, for the log message
     */
    private void retry(BatchResult result, Throwable failure, String reason) {
        TopicPartition partition = result.partition();
        long offset = result.nextOffset();
        if (result.index() > 0) {
            // A record before this one succeeded, so it is not the record retried before.
            attempts.remove(partition);
        }
        int attempt = attempts.merge(partition, 1, Integer::sum);
        long delay = backoff(attempt);
        logger.warn("Processing the record at offset [{}] of partition [{}] {} (attempt {}). Retrying in {} ms.",
                    offset, partition, reason, attempt, delay, failure);
        stoppedPartitions.put(partition, new StoppedPartition(
                partition, offset, StoppedPartition.Reason.RETRYING, failure, attempt
        ));
        // Keeps the progress of the records before the retried one.
        commit(partition, offset);
        retryAt(partition, offset, delay);
    }

    private void handleUndeserializableRecord(RecordDeserializationException e) {
        TopicPartition partition = e.topicPartition();
        long offset = e.offset();
        try {
            configuration.undecodableRecordPolicy().handle(partition, offset, e);
        } catch (UndecodableRecordException rejected) {
            if (!inFlight.containsKey(partition)) {
                stopAt(partition, offset, rejected);
            }
            // Otherwise, the consumer raises the same exception once the events in progress are done.
            return;
        }
        // The consumer cannot move past a record it cannot deserialize, so the position is moved explicitly.
        if (!inFlight.containsKey(partition)) {
            continueAfter(partition, offset);
        } else if (owns(partition)) {
            consumer().seek(partition, offset + 1);
        }
    }

    /**
     * Applies the {@link RetentionGapPolicy} to partitions whose committed offset no longer exists in the partition,
     * usually because Kafka has deleted its records under the topic's retention settings. Under
     * {@link RetentionGapPolicy#FAIL}, the partition stays paused, without committing, until it is assigned again; it
     * then hits the gap again, until the consumer group's offsets are reset.
     */
    private void handleRetentionGap(OffsetOutOfRangeException e) {
        for (Map.Entry<TopicPartition, Long> gap : e.offsetOutOfRangePartitions().entrySet()) {
            TopicPartition partition = gap.getKey();
            if (configuration.retentionGapPolicy() == RetentionGapPolicy.RESET_TO_EARLIEST) {
                logger.warn("The committed offset [{}] of partition [{}] in consumer group [{}] no longer exists in "
                                    + "the partition, for example because Kafka deleted its records under the topic's "
                                    + "retention settings, or because the topic was deleted and created again. "
                                    + "Continuing from the earliest available record.",
                            gap.getValue(), partition, configuration.groupId());
                stoppedPartitions.remove(partition);
                if (owns(partition)) {
                    consumer().seekToBeginning(Set.of(partition));
                }
            } else {
                logger.error("Stopped consuming partition [{}] in consumer group [{}]: its committed offset [{}] no "
                                     + "longer exists in the partition, for example because Kafka deleted its records "
                                     + "under the topic's retention settings, or because the topic was deleted and "
                                     + "created again. Reset the consumer group's offsets to continue, or configure "
                                     + "the RESET_TO_EARLIEST retention gap policy.",
                             partition, configuration.groupId(), gap.getValue());
                resumeAt.remove(partition);
                attempts.remove(partition);
                stoppedPartitions.put(partition, new StoppedPartition(
                        partition, gap.getValue(), StoppedPartition.Reason.RETENTION_GAP, e, 0
                ));
                if (owns(partition)) {
                    consumer().pause(Set.of(partition));
                }
            }
        }
    }

    /**
     * Stops consuming the given {@code partition} at the record at the given {@code offset}, which the
     * undecodable-record policy rejected. Offsets are committed up to that record, never past it, and the partition
     * stays paused until it is assigned again, for example after a restart. Other partitions are not affected.
     */
    private void stopAt(TopicPartition partition, long offset, Throwable rejection) {
        logger.error("Stopped consuming partition [{}] at the record at offset [{}]: it cannot be decoded, and the "
                             + "undecodable-record policy is FAIL. The partition is consumed again from this record "
                             + "when it is assigned again, for example after a restart.",
                     partition, offset, rejection);
        attempts.remove(partition);
        resumeAt.remove(partition);
        stoppedPartitions.put(partition, new StoppedPartition(
                partition, offset, StoppedPartition.Reason.UNDECODABLE_RECORD, rejection, 0
        ));
        commit(partition, offset);
        if (owns(partition)) {
            consumer().pause(Set.of(partition));
            consumer().seek(partition, offset);
        }
    }

    private long backoff(int attempt) {
        long initial = configuration.retryInitialBackoff().toMillis();
        long max = configuration.retryMaxBackoff().toMillis();
        long delay = initial << Math.min(attempt - 1, 30);
        return delay <= 0 ? max : Math.min(delay, max);
    }

    private void retryAt(TopicPartition partition, long offset, long delay) {
        if (owns(partition)) {
            consumer().pause(Set.of(partition));
            consumer().seek(partition, offset);
            resumeAt.put(partition, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay));
        }
    }

    private void continueAfter(TopicPartition partition, long offset) {
        attempts.remove(partition);
        stoppedPartitions.remove(partition);
        commit(partition, offset + 1);
        if (owns(partition)) {
            consumer().seek(partition, offset + 1);
        }
        resume(partition);
    }

    private void resumeDuePartitions() {
        long now = System.nanoTime();
        resumeAt.entrySet().removeIf(entry -> {
            if (entry.getValue() - now <= 0) {
                resume(entry.getKey());
                return true;
            }
            return false;
        });
    }

    private boolean owns(TopicPartition partition) {
        return consumer().assignment().contains(partition);
    }

    private void resume(TopicPartition partition) {
        if (owns(partition)) {
            consumer().resume(Set.of(partition));
        }
    }

    /**
     * Marks the given offset for the next commit, which {@link #processCompletions(int)} makes for all partitions at
     * once.
     */
    private void commit(TopicPartition partition, long nextOffset) {
        pendingCommits.put(partition, new OffsetAndMetadata(nextOffset));
    }

    /**
     * Commits the offsets marked since the last commit, in one request. A commit that fails is not retried: the
     * records it covers are delivered again, to this member or to the partitions' next owners.
     */
    private void flushCommits() {
        if (pendingCommits.isEmpty()) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = Map.copyOf(pendingCommits);
        pendingCommits.clear();
        try {
            try {
                consumer().commitSync(offsets, commitTimeout());
            } catch (WakeupException e) {
                // A stop request, or a completed batch, woke the consumer up outside of poll; the progress is still
                // worth committing.
                consumer().commitSync(offsets, commitTimeout());
            }
        } catch (RetriableException | RebalanceInProgressException | CommitFailedException | WakeupException e) {
            logger.warn("Committing offsets {} failed. The records before them may be delivered again.", offsets, e);
        }
    }

    /**
     * Returns how long the next commit may take: the commit timeout, or, once the member is asked to stop, what is
     * left of the commit timeout after the stop deadline, so that all commits while stopping together stay within the
     * member's stop timeout. A commit that runs out of time is not fatal: the records it covers are delivered again.
     */
    private Duration commitTimeout() {
        return stop.remainingBeyondDeadline(ShutdownBudget.COMMIT_TIMEOUT);
    }

    private void awaitInFlight(Collection<TopicPartition> partitions) {
        CompletableFuture<?>[] results = partitions.stream()
                                                   .map(inFlight::get)
                                                   .filter(Objects::nonNull)
                                                   .map(batch -> batch.result)
                                                   .filter(Objects::nonNull)
                                                   .toArray(CompletableFuture<?>[]::new);
        // Once the member is asked to stop, the wait ends at the stop deadline at the latest, also when it began
        // before the stop request, so that all waits while stopping together stay within the revocation timeout.
        if (!stop.await(CompletableFuture.allOf(results), configuration.revocationTimeout())) {
            logger.debug("Events of partitions {} did not finish in time; they will be redelivered.", partitions);
        }
    }

    private void forget(Collection<TopicPartition> partitions) {
        partitions.forEach(partition -> {
            Batch batch = inFlight.remove(partition);
            if (batch != null) {
                // Processing stops after the record in progress, so that only the partition's next owner continues.
                batch.cancelled = true;
            }
            resumeAt.remove(partition);
            attempts.remove(partition);
            claimedUpTo.remove(partition);
            pendingCommits.remove(partition);
            stoppedPartitions.remove(partition);
        });
    }

    /**
     * Commits the progress of partitions that Kafka takes away, after waiting a bounded time for their events in
     * progress. Runs on the polling thread, inside {@link Consumer#poll}.
     */
    private class RebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // The thread waits for the events in progress itself; waking it up would only interrupt its commit.
            wakeUpOnCompletion.set(false);
            // The batches end after the record in progress, with their exact progress, so that the wait covers one
            // record per partition, and the next owner continues right after it.
            partitions.forEach(partition -> {
                Batch batch = inFlight.get(partition);
                if (batch != null) {
                    batch.cancelled = true;
                }
            });
            awaitInFlight(partitions);
            // Writing to dead-letter topics would extend the rebalance; the next owner deals with those records.
            processCompletions(0);
            forget(partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            // Positions come from the group's committed offsets.
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            // Another member owns these partitions already: committing is no longer possible.
            forget(partitions);
        }
    }
}
