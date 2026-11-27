/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertStrictPositive;

/**
 * Configuration of a {@link MongoSequencedDeadLetterQueue}.
 * <p>
 * The configuration is immutable; every {@code with}-style method returns a new instance. Start from {@link #DEFAULT}
 * and adjust the properties that need to differ:
 * <pre>{@code
 * MongoSequencedDeadLetterQueueConfiguration configuration =
 *         MongoSequencedDeadLetterQueueConfiguration.DEFAULT
 *                 .maxSequences(256)
 *                 .claimDuration(Duration.ofMinutes(1));
 * }</pre>
 * Read the dead-letter queue documentation on capacity and overflow before changing the {@code maxSequences} or
 * {@code maxSequenceSize}, as exceeding either limit stalls a segment of the event processor.
 *
 * @param converter       the {@link DeadLetterMongoConverter} to convert {@link EventMessage EventMessages} to and from
 *                        {@link DeadLetterEventEntry DeadLetterEventEntries}
 * @param maxSequences    the maximum number of unique sequences the queue may contain
 * @param maxSequenceSize the maximum amount of dead letters per unique sequence
 * @param queryPageSize   the number of letters retrieved per query when iterating a sequence of dead letters
 * @param claimDuration   the time a letter stays claimed while it is being processed
 * @param ensureIndexes   whether the {@link MongoSequencedDeadLetterQueue} creates the indexes of the dead letter
 *                        collection upon construction
 * @param allowDiskUse    whether the aggregations that select the first dead letter of every sequence may spill to
 *                        disk
 * @author Steven van Beelen
 * @since 5.5.0
 */
public record MongoSequencedDeadLetterQueueConfiguration(
        DeadLetterMongoConverter<EventMessage> converter,
        int maxSequences,
        int maxSequenceSize,
        int queryPageSize,
        Duration claimDuration,
        boolean ensureIndexes,
        boolean allowDiskUse
) {

    /**
     * The default configuration: the {@link EventMessageDeadLetterMongoConverter}, {@code 1024} sequences of at most
     * {@code 1024} dead letters, a query page size of {@code 100}, a claim duration of 30 seconds, index creation
     * enabled, and disk use allowed for the aggregations.
     */
    public static final MongoSequencedDeadLetterQueueConfiguration DEFAULT =
            new MongoSequencedDeadLetterQueueConfiguration(new EventMessageDeadLetterMongoConverter(),
                                                           1024,
                                                           1024,
                                                           100,
                                                           Duration.ofSeconds(30),
                                                           true,
                                                           true);

    /**
     * Compact constructor validating the given properties.
     *
     * @param converter       the converter, may not be {@code null}
     * @param maxSequences    the maximum number of sequences, strictly positive
     * @param maxSequenceSize the maximum amount of letters per sequence, strictly positive
     * @param queryPageSize   the query page size, strictly positive
     * @param claimDuration   the claim duration, may not be {@code null}
     * @param ensureIndexes   whether to create the required indexes upon construction of the queue
     * @param allowDiskUse    whether the aggregations may spill to disk
     */
    public MongoSequencedDeadLetterQueueConfiguration {
        assertNonNull(converter, "The DeadLetterMongoConverter may not be null.");
        assertStrictPositive(maxSequences, "The maximum number of sequences should be greater than 0");
        assertStrictPositive(maxSequenceSize, "The maximum number of entries in a sequence should be greater than 0");
        assertStrictPositive(queryPageSize, "The query page size must be at least 1.");
        assertNonNull(claimDuration, "Claim duration can not be set to null.");
    }

    /**
     * Returns a copy of this configuration with the given {@code converter}. Defaults to the
     * {@link EventMessageDeadLetterMongoConverter}.
     *
     * @param converter the converter between an {@link EventMessage} and its stored {@link DeadLetterEventEntry}
     * @return a copy of this configuration with the given {@code converter}
     */
    public MongoSequencedDeadLetterQueueConfiguration converter(DeadLetterMongoConverter<EventMessage> converter) {
        return new MongoSequencedDeadLetterQueueConfiguration(converter,
                                                              maxSequences,
                                                              maxSequenceSize,
                                                              queryPageSize,
                                                              claimDuration,
                                                              ensureIndexes,
                                                              allowDiskUse);
    }

    /**
     * Returns a copy of this configuration with the given {@code maxSequences}, the maximum number of unique sequences
     * the queue may contain. It is required to be strictly positive. Defaults to {@code 1024}.
     *
     * @param maxSequences the maximum amount of unique sequences
     * @return a copy of this configuration with the given {@code maxSequences}
     */
    public MongoSequencedDeadLetterQueueConfiguration maxSequences(int maxSequences) {
        return new MongoSequencedDeadLetterQueueConfiguration(converter,
                                                              maxSequences,
                                                              maxSequenceSize,
                                                              queryPageSize,
                                                              claimDuration,
                                                              ensureIndexes,
                                                              allowDiskUse);
    }

    /**
     * Returns a copy of this configuration with the given {@code maxSequenceSize}, the maximum amount of dead letters
     * per unique sequence. It is required to be strictly positive. Defaults to {@code 1024}.
     *
     * @param maxSequenceSize the maximum amount of dead letters per unique sequence
     * @return a copy of this configuration with the given {@code maxSequenceSize}
     */
    public MongoSequencedDeadLetterQueueConfiguration maxSequenceSize(int maxSequenceSize) {
        return new MongoSequencedDeadLetterQueueConfiguration(converter,
                                                              maxSequences,
                                                              maxSequenceSize,
                                                              queryPageSize,
                                                              claimDuration,
                                                              ensureIndexes,
                                                              allowDiskUse);
    }

    /**
     * Returns a copy of this configuration with the given {@code queryPageSize}, the number of letters retrieved per
     * query when iterating a sequence of dead letters. It is required to be strictly positive. Defaults to
     * {@code 100}.
     *
     * @param queryPageSize the number of letters per page
     * @return a copy of this configuration with the given {@code queryPageSize}
     */
    public MongoSequencedDeadLetterQueueConfiguration queryPageSize(int queryPageSize) {
        return new MongoSequencedDeadLetterQueueConfiguration(converter,
                                                              maxSequences,
                                                              maxSequenceSize,
                                                              queryPageSize,
                                                              claimDuration,
                                                              ensureIndexes,
                                                              allowDiskUse);
    }

    /**
     * Returns a copy of this configuration with the given {@code claimDuration}, the time a letter stays claimed while
     * it is being processed. Other invocations of the {@code process} methods are unable to process its sequence while
     * the claim is active. Claims are released once the letter is requeued, so the claim duration is a backup policy
     * for unforeseen trouble such as down database connections. Defaults to 30 seconds.
     *
     * @param claimDuration the longest claim duration allowed
     * @return a copy of this configuration with the given {@code claimDuration}
     */
    public MongoSequencedDeadLetterQueueConfiguration claimDuration(Duration claimDuration) {
        return new MongoSequencedDeadLetterQueueConfiguration(converter,
                                                              maxSequences,
                                                              maxSequenceSize,
                                                              queryPageSize,
                                                              claimDuration,
                                                              ensureIndexes,
                                                              allowDiskUse);
    }

    /**
     * Returns a copy of this configuration with the given {@code ensureIndexes} flag. When {@code false}, the developer
     * is responsible for creating the indexes of the dead letter collection beforehand, which are described in the
     * documentation of the MongoDB dead-letter queue. Defaults to {@code true}.
     *
     * @param ensureIndexes whether the queue creates the required indexes upon construction
     * @return a copy of this configuration with the given {@code ensureIndexes} flag
     */
    public MongoSequencedDeadLetterQueueConfiguration ensureIndexes(boolean ensureIndexes) {
        return new MongoSequencedDeadLetterQueueConfiguration(converter,
                                                              maxSequences,
                                                              maxSequenceSize,
                                                              queryPageSize,
                                                              claimDuration,
                                                              ensureIndexes,
                                                              allowDiskUse);
    }

    /**
     * Returns a copy of this configuration with the given {@code allowDiskUse} flag. The queue selects the first dead
     * letter of every sequence through an aggregation that sorts the dead letters of the processing group. MongoDB
     * limits the memory of such a sort to 100 megabytes and fails the aggregation when it does not fit. When
     * {@code true}, the aggregation may spill to disk instead, at the cost of speed once the limit is reached. When
     * {@code false}, a large dead letter collection may make these aggregations fail. Defaults to {@code true}.
     *
     * @param allowDiskUse whether the aggregations may spill to disk
     * @return a copy of this configuration with the given {@code allowDiskUse} flag
     */
    public MongoSequencedDeadLetterQueueConfiguration allowDiskUse(boolean allowDiskUse) {
        return new MongoSequencedDeadLetterQueueConfiguration(converter,
                                                              maxSequences,
                                                              maxSequenceSize,
                                                              queryPageSize,
                                                              claimDuration,
                                                              ensureIndexes,
                                                              allowDiskUse);
    }
}
