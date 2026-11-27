/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import io.axoniq.framework.messaging.deadletter.Cause;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Metadata;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static io.axoniq.framework.mongo.deadletter.InstantEntry.NANOSECONDS_KEY;
import static io.axoniq.framework.mongo.deadletter.InstantEntry.SECONDS_KEY;

/**
 * Default dead letter entity stored in MongoDB by the {@link MongoSequencedDeadLetterQueue}, to be retried later.
 * <p>
 * The original message is embedded as a {@link DeadLetterEventEntry}. This is mapped, upon both storage and retrieval,
 * by the configured {@link DeadLetterMongoConverter}.
 * <p>
 * Besides the entity itself, this class provides the {@link Bson} filters, updates, sorts, and aggregation pipelines
 * used to query the dead letter collection.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class DeadLetterEntry {

    private static final int ORDER_ASC = 1;

    private static final String PROCESSING_GROUP_KEY = "processingGroup";
    private static final String SEQUENCE_IDENTIFIER_KEY = "sequenceIdentifier";
    private static final String INDEX_KEY = "index";
    private static final String MESSAGE_KEY = "message";
    private static final String ENQUEUED_AT_KEY = "enqueuedAt";
    private static final String LAST_TOUCHED_KEY = "lastTouched";
    private static final String LAST_TOUCHED_SECONDS_KEY = LAST_TOUCHED_KEY + "." + SECONDS_KEY;
    private static final String LAST_TOUCHED_NANOSECONDS_KEY = LAST_TOUCHED_KEY + "." + NANOSECONDS_KEY;
    private static final String PROCESSING_STARTED_KEY = "processingStarted";
    private static final String PROCESSING_STARTED_SECONDS_KEY = PROCESSING_STARTED_KEY + "." + SECONDS_KEY;
    private static final String PROCESSING_STARTED_NANOSECONDS_KEY = PROCESSING_STARTED_KEY + "." + NANOSECONDS_KEY;
    private static final String CAUSE_TYPE_KEY = "causeType";
    private static final String CAUSE_MESSAGE_KEY = "causeMessage";
    private static final String DIAGNOSTICS_KEY = "diagnostics";
    private static final String FIRST_KEY = "first";

    private final String processingGroup;
    private final String sequenceIdentifier;
    private final long index;
    private final Document message;
    private final Document enqueuedAt;
    private final Document lastTouched;
    private final @Nullable Document processingStarted;
    private final @Nullable String causeType;
    private final @Nullable String causeMessage;
    private final byte[] diagnostics;

    /**
     * Initializes a {@code DeadLetterEntry} using the MongoDB {@link Document} containing the stored dead letter.
     *
     * @param dbDeadLetterEntry the {@link Document} containing the stored dead letter
     */
    public DeadLetterEntry(Document dbDeadLetterEntry) {
        this.processingGroup = Objects.requireNonNull(dbDeadLetterEntry.getString(PROCESSING_GROUP_KEY));
        this.sequenceIdentifier = Objects.requireNonNull(dbDeadLetterEntry.getString(SEQUENCE_IDENTIFIER_KEY));
        this.index = dbDeadLetterEntry.getLong(INDEX_KEY);
        this.message = Objects.requireNonNull((Document) dbDeadLetterEntry.get(MESSAGE_KEY));
        this.enqueuedAt = Objects.requireNonNull((Document) dbDeadLetterEntry.get(ENQUEUED_AT_KEY));
        this.lastTouched = Objects.requireNonNull((Document) dbDeadLetterEntry.get(LAST_TOUCHED_KEY));
        this.processingStarted = (Document) dbDeadLetterEntry.get(PROCESSING_STARTED_KEY);
        this.causeType = dbDeadLetterEntry.getString(CAUSE_TYPE_KEY);
        this.causeMessage = dbDeadLetterEntry.getString(CAUSE_MESSAGE_KEY);
        this.diagnostics = Objects.requireNonNull(toBytes(dbDeadLetterEntry.get(DIAGNOSTICS_KEY)));
    }

    /**
     * Creates a new {@code DeadLetterEntry} consisting of the given parameters.
     *
     * @param processingGroup    the processing group this message belongs to
     * @param sequenceIdentifier the sequence identifier this message belongs to
     * @param index              the index of this message within the sequence
     * @param message            the message stored as {@link Document}
     * @param enqueuedAt         the time the message was enqueued
     * @param lastTouched        the time the message has been last processed
     * @param cause              the reason the message was enqueued
     * @param diagnostics        the diagnostics, a map of metadata
     * @param converter          the {@link Converter} to convert the diagnostics with
     */
    public DeadLetterEntry(
            String processingGroup,
            String sequenceIdentifier,
            long index,
            Document message,
            Instant enqueuedAt,
            Instant lastTouched,
            @Nullable Cause cause,
            Metadata diagnostics,
            Converter converter
    ) {
        this.processingGroup = processingGroup;
        this.sequenceIdentifier = sequenceIdentifier;
        this.index = index;
        this.message = message;
        this.enqueuedAt = new InstantEntry(enqueuedAt).asDocument();
        this.lastTouched = new InstantEntry(lastTouched).asDocument();
        this.processingStarted = null;
        this.causeType = cause != null ? cause.type() : null;
        this.causeMessage = cause != null ? cause.message() : null;
        this.diagnostics = convertDiagnostics(diagnostics, converter);
    }

    private static byte @Nullable [] toBytes(@Nullable Object value) {
        if (value instanceof Binary binary) {
            return binary.getData();
        }
        return (byte[]) value;
    }

    /**
     * Converts the given {@code diagnostics} to the byte array in which they are stored.
     *
     * @param diagnostics the diagnostics to convert
     * @param converter   the {@link Converter} to convert the diagnostics with
     * @return the converted diagnostics
     */
    public static byte[] convertDiagnostics(Metadata diagnostics, Converter converter) {
        return Objects.requireNonNull(converter.convert(new HashMap<>(diagnostics), byte[].class));
    }

    /**
     * Returns the current dead letter as a MongoDB {@link Document}.
     *
     * @return a {@link Document} representing this entry
     */
    public Document asDocument() {
        return new Document().append(PROCESSING_GROUP_KEY, processingGroup)
                             .append(SEQUENCE_IDENTIFIER_KEY, sequenceIdentifier)
                             .append(INDEX_KEY, index)
                             .append(MESSAGE_KEY, message)
                             .append(ENQUEUED_AT_KEY, enqueuedAt)
                             .append(LAST_TOUCHED_KEY, lastTouched)
                             .append(PROCESSING_STARTED_KEY, processingStarted)
                             .append(CAUSE_TYPE_KEY, causeType)
                             .append(CAUSE_MESSAGE_KEY, causeMessage)
                             .append(DIAGNOSTICS_KEY, diagnostics);
    }

    /**
     * The processing group this dead letter belongs to.
     *
     * @return the processing group
     */
    public String getProcessingGroup() {
        return processingGroup;
    }

    /**
     * The sequence identifier of this dead letter. If two letters share the same identifier, they must be handled
     * sequentially.
     *
     * @return the sequence identifier
     */
    public String getSequenceIdentifier() {
        return sequenceIdentifier;
    }

    /**
     * The index of this message within the {@link #getSequenceIdentifier() sequence}, used for keeping the messages in
     * the same order within the same sequence.
     *
     * @return the index
     */
    public long getIndex() {
        return index;
    }

    /**
     * The embedded {@link DeadLetterEventEntry} representing the original message.
     *
     * @return the embedded original message
     */
    public DeadLetterEventEntry getMessage() {
        return new DeadLetterEventEntry(message);
    }

    /**
     * The time the message was enqueued.
     *
     * @return the time the message was enqueued
     */
    public Instant getEnqueuedAt() {
        return new InstantEntry(enqueuedAt).getInstant();
    }

    /**
     * The time the message was last touched, meaning it was queued or an attempt was made to process it.
     *
     * @return the time the message was last touched
     */
    public Instant getLastTouched() {
        return new InstantEntry(lastTouched).getInstant();
    }

    /**
     * Timestamp indicating when the processing of this dead letter has started. Used for claiming messages and
     * preventing multiple processes or threads from handling items concurrently within the same sequence.
     *
     * @return the timestamp of the start of processing, or {@code null} if the letter is not claimed
     */
    @Nullable
    public Instant getProcessingStarted() {
        return Optional.ofNullable(processingStarted)
                       .map(InstantEntry::new)
                       .map(InstantEntry::getInstant)
                       .orElse(null);
    }

    /**
     * Gets the class name of the original exception.
     *
     * @return the type of the cause, or {@code null} if the letter has no cause
     */
    @Nullable
    public String getCauseType() {
        return causeType;
    }

    /**
     * Gets the message of the original exception.
     *
     * @return the message of the cause, or {@code null} if the letter has no cause
     */
    @Nullable
    public String getCauseMessage() {
        return causeMessage;
    }

    /**
     * Returns the converted diagnostics.
     *
     * @return the converted diagnostics
     */
    public byte[] getDiagnostics() {
        return diagnostics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeadLetterEntry that = (DeadLetterEntry) o;
        return index == that.index
                && Objects.equals(processingGroup, that.processingGroup)
                && Objects.equals(sequenceIdentifier, that.sequenceIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processingGroup, sequenceIdentifier, index);
    }

    @Override
    public String toString() {
        return "DeadLetterEntry{" +
                "processingGroup='" + processingGroup + '\'' +
                ", sequenceIdentifier='" + sequenceIdentifier + '\'' +
                ", index=" + index +
                ", message=" + message +
                ", enqueuedAt=" + enqueuedAt +
                ", lastTouched=" + lastTouched +
                ", processingStarted=" + processingStarted +
                ", causeType='" + causeType + '\'' +
                ", causeMessage='" + causeMessage + '\'' +
                ", diagnostics=" + Arrays.toString(diagnostics) +
                '}';
    }

    /**
     * Creates the indexes needed for the dead letter collection.
     *
     * @param session              the {@link ClientSession} to create the indexes with
     * @param deadLetterCollection the {@link MongoCollection} to create the indexes on
     */
    public static void ensureDeadLetterIndexes(ClientSession session,
                                               MongoCollection<Document> deadLetterCollection) {
        deadLetterCollection.createIndex(
                session,
                new Document(PROCESSING_GROUP_KEY, ORDER_ASC),
                new IndexOptions().unique(false)
                                  .name("processingGroupIdentifierIndex")
        );
        deadLetterCollection.createIndex(
                session,
                new Document(PROCESSING_GROUP_KEY, ORDER_ASC).append(SEQUENCE_IDENTIFIER_KEY, ORDER_ASC),
                new IndexOptions().unique(false)
                                  .name("processingGroupAndSequenceIdentifierIndex")
        );
        deadLetterCollection.createIndex(
                session,
                new Document(PROCESSING_GROUP_KEY, ORDER_ASC).append(SEQUENCE_IDENTIFIER_KEY, ORDER_ASC)
                                                             .append(INDEX_KEY, ORDER_ASC),
                new IndexOptions().unique(true)
                                  .name("uniqueDeadLetterEntryIndex")
        );
    }

    /**
     * Filter on the {@code processingGroup} only.
     *
     * @param processingGroup the processing group
     * @return a {@link Bson} filter
     */
    public static Bson processingGroupFilter(String processingGroup) {
        return eq(PROCESSING_GROUP_KEY, processingGroup);
    }

    /**
     * Filter on the {@code processingGroup} and {@code sequenceIdentifier}.
     *
     * @param processingGroup    the processing group
     * @param sequenceIdentifier the sequence identifier
     * @return a {@link Bson} filter
     */
    public static Bson processingGroupAndSequenceIdentifierFilter(String processingGroup,
                                                                  String sequenceIdentifier) {
        return and(eq(PROCESSING_GROUP_KEY, processingGroup),
                   eq(SEQUENCE_IDENTIFIER_KEY, sequenceIdentifier));
    }

    /**
     * Filter on the {@code processingGroup}, {@code sequenceIdentifier} and {@code index}.
     *
     * @param processingGroup    the processing group
     * @param sequenceIdentifier the sequence identifier
     * @param index              the index
     * @return a {@link Bson} filter
     */
    public static Bson findOneFilter(String processingGroup,
                                     String sequenceIdentifier,
                                     long index) {
        return and(eq(PROCESSING_GROUP_KEY, processingGroup),
                   eq(SEQUENCE_IDENTIFIER_KEY, sequenceIdentifier),
                   eq(INDEX_KEY, index));
    }

    /**
     * Filter on the {@code processingGroup} and {@code sequenceIdentifier}, where the index needs to be higher than the
     * provided {@code index}.
     *
     * @param processingGroup    the processing group
     * @param sequenceIdentifier the sequence identifier
     * @param index              the index
     * @return a {@link Bson} filter
     */
    public static Bson nextItemInSequenceFilter(String processingGroup,
                                                String sequenceIdentifier,
                                                long index) {
        return and(eq(PROCESSING_GROUP_KEY, processingGroup),
                   eq(SEQUENCE_IDENTIFIER_KEY, sequenceIdentifier),
                   gt(INDEX_KEY, index));
    }

    /**
     * Filter on the {@code processingGroup}, {@code sequenceIdentifier}, {@code index} and {@code processingStarted}.
     * The first three should match, while the {@code processingStarted} is either absent or earlier in time than the
     * given {@code processingStartedLimit}.
     *
     * @param processingGroup        the processing group
     * @param sequenceIdentifier     the sequence identifier
     * @param index                  the index
     * @param processingStartedLimit the processing started limit
     * @return a {@link Bson} filter
     */
    public static Bson uniqueNotLockedFilter(String processingGroup,
                                             String sequenceIdentifier,
                                             long index,
                                             Instant processingStartedLimit) {
        return and(findOneFilter(processingGroup, sequenceIdentifier, index),
                   notLockedFilter(processingStartedLimit));
    }

    private static Bson notLockedFilter(Instant processingStartedLimit) {
        return or(
                exists(PROCESSING_STARTED_SECONDS_KEY, false),
                lt(PROCESSING_STARTED_SECONDS_KEY, processingStartedLimit.getEpochSecond()),
                and(
                        eq(PROCESSING_STARTED_SECONDS_KEY, processingStartedLimit.getEpochSecond()),
                        lt(PROCESSING_STARTED_NANOSECONDS_KEY, processingStartedLimit.getNano())
                )
        );
    }

    /**
     * Provides a pipeline for an aggregation that returns, for each sequence of the given {@code processingGroup}, a
     * document containing the {@code _id}, {@code sequenceIdentifier}, {@code index}, {@code lastTouched}, and
     * {@code processingStarted} of the letter with the lowest index. The result is sorted by last touched.
     * <p>
     * When a {@code processingStartedLimit} is given, only the sequences of which that first letter can be claimed are
     * returned.
     *
     * @param processingGroup        the processing group
     * @param processingStartedLimit the processing started limit, or {@code null} to include claimed letters as well
     * @return a list of {@link Bson} stages, to be used in {@link MongoCollection#aggregate(List)}
     */
    public static List<Bson> firstOfEachSequencePipeline(String processingGroup,
                                                         @Nullable Instant processingStartedLimit) {
        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(Aggregates.match(processingGroupFilter(processingGroup)));
        // Only the lightweight fields are kept, to keep the sort within memory limits.
        pipeline.add(Aggregates.project(Projections.include(SEQUENCE_IDENTIFIER_KEY,
                                                            INDEX_KEY,
                                                            LAST_TOUCHED_KEY,
                                                            PROCESSING_STARTED_KEY)));
        pipeline.add(Aggregates.sort(ascending(INDEX_KEY)));
        pipeline.add(Aggregates.group("$" + SEQUENCE_IDENTIFIER_KEY, Accumulators.first(FIRST_KEY, "$$ROOT")));
        pipeline.add(Aggregates.replaceRoot("$" + FIRST_KEY));
        if (processingStartedLimit != null) {
            pipeline.add(Aggregates.match(notLockedFilter(processingStartedLimit)));
        }
        pipeline.add(Aggregates.sort(ascending(LAST_TOUCHED_SECONDS_KEY, LAST_TOUCHED_NANOSECONDS_KEY)));
        return pipeline;
    }

    /**
     * {@link Bson} update claiming a letter by setting its {@code processingStarted} to the given {@code now}.
     *
     * @param now the current time
     * @return a {@link Bson} update
     */
    public static Bson updateProcessingStarted(Instant now) {
        return Updates.set(PROCESSING_STARTED_KEY, new InstantEntry(now).asDocument());
    }

    /**
     * {@link Bson} update that requeues a letter: it replaces the cause, diagnostics, and last touched time, and
     * releases the claim by clearing the {@code processingStarted}.
     *
     * @param lastTouched the new last touched time
     * @param cause       the new cause, or {@code null} to clear it
     * @param diagnostics the new converted diagnostics
     * @return a {@link Bson} update
     */
    public static Bson requeueUpdate(Instant lastTouched,
                                     @Nullable Cause cause,
                                     byte[] diagnostics) {
        return Updates.combine(
                Updates.set(LAST_TOUCHED_KEY, new InstantEntry(lastTouched).asDocument()),
                Updates.set(CAUSE_TYPE_KEY, cause != null ? cause.type() : null),
                Updates.set(CAUSE_MESSAGE_KEY, cause != null ? cause.message() : null),
                Updates.set(DIAGNOSTICS_KEY, diagnostics),
                Updates.set(PROCESSING_STARTED_KEY, null)
        );
    }

    /**
     * Gets an iterator over all distinct sequence identifiers of the given {@code processingGroup}. There are no
     * guarantees on the ordering.
     *
     * @param session         the {@link ClientSession} to select the distinct sequence identifiers with
     * @param collection      the {@link MongoCollection} to select the distinct sequence identifiers from
     * @param processingGroup the processing group
     * @return a {@link DistinctIterable} with sequence identifiers
     */
    public static DistinctIterable<String> sequenceIdentifierIterator(ClientSession session,
                                                                      MongoCollection<Document> collection,
                                                                      String processingGroup) {
        return collection.distinct(
                session,
                SEQUENCE_IDENTIFIER_KEY,
                processingGroupFilter(processingGroup),
                String.class
        );
    }

    /**
     * Sorts by index descending, so the highest first.
     *
     * @return a {@link Bson} sort
     */
    public static Bson indexSortDescending() {
        return descending(INDEX_KEY);
    }

    /**
     * Sorts by index ascending, so the lowest first.
     *
     * @return a {@link Bson} sort
     */
    public static Bson indexSortAscending() {
        return ascending(INDEX_KEY);
    }

    /**
     * Returns the index if present on the document.
     *
     * @param document a {@link Document} which is expected to be a {@code DeadLetterEntry} representation
     * @return the index, or {@code null} when the document is {@code null} or does not contain an index
     */
    @Nullable
    public static Long index(@Nullable Document document) {
        return Optional.ofNullable(document)
                       .map(d -> d.getLong(INDEX_KEY))
                       .orElse(null);
    }
}
