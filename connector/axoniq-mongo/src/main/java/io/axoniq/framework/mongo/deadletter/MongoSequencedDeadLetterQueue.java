/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import io.axoniq.framework.messaging.deadletter.Cause;
import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.DeadLetterQueueOverflowException;
import io.axoniq.framework.messaging.deadletter.EnqueueDecision;
import io.axoniq.framework.messaging.deadletter.GenericDeadLetter;
import io.axoniq.framework.messaging.deadletter.NoSuchDeadLetterException;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.messaging.deadletter.WrongDeadLetterTypeException;
import io.axoniq.framework.mongo.MongoAxoniqAddon;
import io.axoniq.framework.mongo.transaction.ClientSessionTransactionManager;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.TypeReference;
import org.axonframework.common.function.ThrowingConsumer;
import org.axonframework.common.function.ThrowingFunction;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static io.axoniq.framework.mongo.deadletter.DeadLetterEntry.*;
import static org.axonframework.common.BuilderUtils.assertNonEmpty;
import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * MongoDB-backed implementation of the {@link SequencedDeadLetterQueue}, used for storing dead letters containing
 * {@link EventMessage EventMessages} durably as a {@link DeadLetterEntry}.
 * <p>
 * Keeps the insertion order intact by saving an incremented index within each unique sequence, backed by the
 * {@link DeadLetterEntry#getIndex()} property. Each sequence is uniquely identified by the sequence identifier, stored
 * in the {@link DeadLetterEntry#getSequenceIdentifier()} field. All dead letters are scoped by the configured
 * {@code processingGroup}.
 * <p>
 * When processing an item, single execution across all applications is guaranteed by atomically setting the
 * {@link DeadLetterEntry#getProcessingStarted()} property, locking other processes out of the sequence for the
 * configured {@code claimDuration} (30 seconds by default). As MongoDB offers atomic single-document operations, the
 * claim is made through an update that only matches when the letter is not claimed (or its claim has expired).
 * <p>
 * The stored {@link DeadLetterEntry entries} are converted to a {@link MongoDeadLetter} when they need to be processed
 * or filtered. In order to restore the original {@link EventMessage} the configured {@link DeadLetterMongoConverter} is
 * used. The default {@link EventMessageDeadLetterMongoConverter} supports all {@code EventMessage} implementations
 * provided by the framework.
 * <p>
 * Every operation resolves the {@link TransactionalExecutor} for the {@link ClientSession} through the configured
 * {@link TransactionalExecutorProvider}, using the {@link ProcessingContext} it is invoked with. When the
 * {@link ClientSessionTransactionManager} is registered as the
 * {@link org.axonframework.messaging.core.unitofwork.transaction.TransactionManager}, enqueueing, requeueing and
 * evicting a dead letter hence take part in the MongoDB transaction of the event handling they belong to, together with
 * the token update and the writes of the event handlers. An operation fails when its {@link ProcessingContext} has no
 * session bound. Note that MongoDB only supports multi-document transactions on a replica set or sharded cluster.
 * <p>
 * The indexes of the dead letter collection are created upon construction, unless
 * {@link MongoSequencedDeadLetterQueueConfiguration#ensureIndexes()} is {@code false}. Creating an index that already
 * exists is a no-op.
 * <p>
 * Constructing this queue requires the Axoniq MongoDB entitlement, see {@link MongoAxoniqAddon}.
 * <p>
 * Example usage:
 * <pre>{@code
 * MongoSequencedDeadLetterQueue<EventMessage> queue = new MongoSequencedDeadLetterQueue<>(
 *         "my-processing-group",
 *         mongoTemplate,
 *         transactionalExecutorProvider,
 *         eventConverter,
 *         genericConverter,
 *         MongoSequencedDeadLetterQueueConfiguration.DEFAULT
 * );
 * }</pre>
 *
 * @param <M> an implementation of {@link Message} contained in the {@link DeadLetter dead-letters} within this queue
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class MongoSequencedDeadLetterQueue<M extends EventMessage> implements SequencedDeadLetterQueue<M> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Duration QUERY_TIMEOUT = Duration.ofSeconds(30);
    // Concrete HashMap i.o. Map: with Jackson default typing, a non-concrete target type expects a type id,
    // which writing a concrete Metadata never adds.
    private static final TypeReference<HashMap<String, String>> DIAGNOSTICS_MAP_TYPE_REF = new TypeReference<>() {
    };

    private final String processingGroup;
    private final MongoCollection<Document> collection;
    private final TransactionalExecutorProvider<ClientSession> transactionalExecutorProvider;
    private final EventConverter eventConverter;
    private final Converter genericConverter;
    private final DeadLetterMongoConverter<EventMessage> converter;
    private final int maxSequences;
    private final int maxSequenceSize;
    private final int queryPageSize;
    private final Duration claimDuration;
    private final boolean allowDiskUse;

    /**
     * Instantiate a MongoDB {@link SequencedDeadLetterQueue} with the given {@code configuration}. Use the
     * {@link MongoSequencedDeadLetterQueueConfiguration#DEFAULT default configuration} when no settings need to
     * differ.
     *
     * @param processingGroup               the processing group, used for storing and querying which event processor
     *                                      the dead-lettered item belonged to
     * @param deadLetterCollection          the {@link MongoCollection} storing the dead letters
     * @param transactionalExecutorProvider the {@link TransactionalExecutorProvider} that provides the
     *                                      {@link ClientSession} every operation of the queue executes with. An
     *                                      operation invoked with a {@link ProcessingContext} uses the session bound to
     *                                      that context, so when the {@link ClientSessionTransactionManager} binds it,
     *                                      a dead letter is enqueued, requeued or evicted in the same MongoDB
     *                                      transaction as the event handling it belongs to
     * @param eventConverter                the {@link EventConverter} to convert the event payload and metadata of a
     *                                      {@link DeadLetter} when storing it to and retrieving it from the database
     * @param genericConverter              the {@link Converter} to convert the tracking token and diagnostics of a
     *                                      {@link DeadLetter} when storing it to and retrieving it from the database
     * @param configuration                 the {@link MongoSequencedDeadLetterQueueConfiguration} holding the settings
     *                                      of the queue, such as its capacity and claim duration
     * @throws AxonConfigurationException when a parameter is {@code null}, or the {@code processingGroup} is empty
     */
    public MongoSequencedDeadLetterQueue(String processingGroup,
                                         MongoCollection<Document> deadLetterCollection,
                                         TransactionalExecutorProvider<ClientSession> transactionalExecutorProvider,
                                         EventConverter eventConverter,
                                         Converter genericConverter,
                                         MongoSequencedDeadLetterQueueConfiguration configuration) {
        assertNonEmpty(processingGroup,
                       "Must supply processingGroup when constructing a MongoSequencedDeadLetterQueue");
        assertNonNull(deadLetterCollection,
                      "Must supply a dead letter collection when constructing a MongoSequencedDeadLetterQueue");
        assertNonNull(transactionalExecutorProvider,
                      "Must supply a TransactionalExecutorProvider when constructing a MongoSequencedDeadLetterQueue");
        assertNonNull(eventConverter,
                      "Must supply an eventConverter when constructing a MongoSequencedDeadLetterQueue");
        assertNonNull(genericConverter,
                      "Must supply a genericConverter when constructing a MongoSequencedDeadLetterQueue");
        assertNonNull(configuration,
                      "Must supply a MongoSequencedDeadLetterQueueConfiguration when constructing a "
                              + "MongoSequencedDeadLetterQueue");

        EntitlementManager.INSTANCE.registerAddon(MongoAxoniqAddon.class);

        this.processingGroup = processingGroup;
        this.collection = deadLetterCollection;
        this.transactionalExecutorProvider = transactionalExecutorProvider;
        this.eventConverter = eventConverter;
        this.genericConverter = genericConverter;
        this.converter = configuration.converter();
        this.maxSequences = configuration.maxSequences();
        this.maxSequenceSize = configuration.maxSequenceSize();
        this.queryPageSize = configuration.queryPageSize();
        this.claimDuration = configuration.claimDuration();
        this.allowDiskUse = configuration.allowDiskUse();

        if (configuration.ensureIndexes()) {
            // Index creation is schema management, so it runs with a session of its own, outside of any processing
            // context.
            FutureUtils.joinAndUnwrap(transactionalExecutorProvider.getTransactionalExecutor(null).accept(
                    session -> DeadLetterEntry.ensureDeadLetterIndexes(session, deadLetterCollection)
            ), QUERY_TIMEOUT);
        }
    }

    @Override
    public CompletableFuture<Void> enqueue(Object sequenceIdentifier,
                                           DeadLetter<? extends M> letter,
                                           @Nullable ProcessingContext context) {
        String stringSequenceIdentifier = toStringSequenceIdentifier(sequenceIdentifier);
        return accept(context, session -> {
            if (isFull(session, stringSequenceIdentifier)) {
                throw new DeadLetterQueueOverflowException(
                        "No room left to enqueue [" + letter.message() + "] for identifier ["
                                + stringSequenceIdentifier + "] since the queue is full."
                );
            }

            Optional<Cause> optionalCause = letter.cause();
            if (optionalCause.isPresent()) {
                logger.info("Adding dead letter with message id [{}] because [{}].",
                            letter.message().identifier(), optionalCause.get().type());
            } else {
                logger.info("Adding dead letter with message id [{}] because the sequence identifier [{}] "
                                    + "is already present.",
                            letter.message().identifier(), stringSequenceIdentifier);
            }

            DeadLetterEventEntry eventEntry = converter.convert(
                    letter.message(), letter.context(), eventConverter, genericConverter
            );
            Document eventDocument = eventEntry.asDocument();
            long sequenceIndex = nextIndexForSequence(session, stringSequenceIdentifier);
            Document deadLetter = new DeadLetterEntry(
                    processingGroup, stringSequenceIdentifier, sequenceIndex, eventDocument, letter.enqueuedAt(),
                    letter.lastTouched(), letter.cause().orElse(null), letter.diagnostics(), genericConverter
            ).asDocument();

            logger.info("Storing DeadLetter (id: [{}]) for sequence [{}] with index [{}] in processing group [{}].",
                        letter.message().identifier(), stringSequenceIdentifier, sequenceIndex, processingGroup);
            collection.insertOne(session, deadLetter);
        });
    }

    /**
     * Fetches the next index that should be used when inserting a new item for the given {@code sequenceIdentifier}.
     *
     * @param session            the {@link ClientSession} to fetch the index with
     * @param sequenceIdentifier the identifier of the sequence to fetch the next index for
     * @return the next sequence index
     */
    private long nextIndexForSequence(ClientSession session, String sequenceIdentifier) {
        Document highest = collection.find(
                                             session,
                                             processingGroupAndSequenceIdentifierFilter(
                                                     processingGroup, sequenceIdentifier
                                             )
                                     )
                                     .sort(indexSortDescending())
                                     .limit(1)
                                     .first();
        Long maxIndex = index(highest);
        return maxIndex == null ? 0L : maxIndex + 1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Void> evict(DeadLetter<? extends M> letter,
                                         @Nullable ProcessingContext context) {
        if (!(letter instanceof MongoDeadLetter)) {
            return CompletableFuture.failedFuture(new WrongDeadLetterTypeException(String.format(
                    "Evict should be called with a MongoDeadLetter instance. Instead got: [%s]",
                    letter.getClass().getName()
            )));
        }

        MongoDeadLetter<M> mongoDeadLetter = (MongoDeadLetter<M>) letter;
        return accept(context, session -> evictLetter(session, mongoDeadLetter));
    }


    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Void> requeue(DeadLetter<? extends M> letter,
                                           UnaryOperator<DeadLetter<? extends M>> letterUpdater,
                                           @Nullable ProcessingContext context) {
        if (!(letter instanceof MongoDeadLetter)) {
            return CompletableFuture.failedFuture(new WrongDeadLetterTypeException(String.format(
                    "Requeue should be called with a MongoDeadLetter instance. Instead got: [%s]",
                    letter.getClass().getName()
            )));
        }
        return accept(context, session -> {
            DeadLetter<? extends M> updatedLetter = letterUpdater.apply(letter).markTouched();
            MongoDeadLetter<M> mongoLetter = (MongoDeadLetter<M>) letter;
            logger.info("Requeueing dead letter for processing group [{}], sequence [{}] and index [{}] "
                                + "with cause [{}]",
                        processingGroup, mongoLetter.sequenceIdentifier(), mongoLetter.index(), updatedLetter.cause());

            UpdateResult result = collection.updateOne(
                    session,
                    findOneFilter(processingGroup, mongoLetter.sequenceIdentifier(), mongoLetter.index()),
                    requeueUpdate(
                            updatedLetter.lastTouched(),
                            updatedLetter.cause().orElse(null),
                            convertDiagnostics(updatedLetter.diagnostics(), genericConverter)
                    )
            );

            if (result.getMatchedCount() == 0) {
                throw new NoSuchDeadLetterException(String.format(
                        "Can not find dead letter with processing group [%s], sequence identifier [%s] "
                                + "and index [%s] to requeue.",
                        processingGroup, mongoLetter.sequenceIdentifier(), mongoLetter.index())
                );
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> contains(Object sequenceIdentifier, @Nullable ProcessingContext context) {
        String stringSequenceIdentifier = toStringSequenceIdentifier(sequenceIdentifier);
        return execute(context, session -> sequenceSize(session, stringSequenceIdentifier) > 0);
    }

    @Override
    public CompletableFuture<Iterable<DeadLetter<? extends M>>> deadLetterSequence(Object sequenceIdentifier,
                                                                                   @Nullable ProcessingContext context) {
        return FutureUtils.runFailing(() -> CompletableFuture.completedFuture(
                deadLetterSequenceIterable(executorFor(context), toStringSequenceIdentifier(sequenceIdentifier))
        ));
    }

    private Iterable<DeadLetter<? extends M>> deadLetterSequenceIterable(
            TransactionalExecutor<ClientSession> executor,
            String sequenceIdentifier
    ) {
        return new PagingMongoIterable<>(
                queryPageSize,
                executor,
                (session, lastEntry, pageSize) -> collection.find(session, nextItemInSequenceFilter(
                                                                    processingGroup,
                                                                    sequenceIdentifier,
                                                                    lastEntry == null ? -1L : lastEntry.getIndex()
                                                            ))
                                                            .sort(indexSortAscending())
                                                            .limit(pageSize)
                                                            .map(DeadLetterEntry::new)
                                                            .into(new ArrayList<>()),
                this::toLetter
        );
    }

    @Override
    public CompletableFuture<Iterable<Iterable<DeadLetter<? extends M>>>> deadLetters(
            @Nullable ProcessingContext context
    ) {
        return FutureUtils.runFailing(() -> {
            TransactionalExecutor<ClientSession> executor = executorFor(context);
            return executor.apply(session -> {
                List<String> sequenceIdentifiers = new ArrayList<>();
                collection.aggregate(session, firstOfEachSequencePipeline(processingGroup, null))
                          .allowDiskUse(allowDiskUse)
                          .forEach(document -> sequenceIdentifiers.add(document.getString(
                                  "sequenceIdentifier"
                          )));
                return sequenceIdentifiers;
            }).thenApply(sequenceIdentifiers -> () -> {
                Iterator<String> sequenceIterator = sequenceIdentifiers.iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return sequenceIterator.hasNext();
                    }

                    @Override
                    public Iterable<DeadLetter<? extends M>> next() {
                        return deadLetterSequenceIterable(executor, sequenceIterator.next());
                    }
                };
            });
        });
    }

    @SuppressWarnings("unchecked") // Safe: M extends EventMessage, and the converter restores the actual runtime type
    private MongoDeadLetter<M> toLetter(DeadLetterEntry entry) {
        Map<String, String> diagnosticsMap =
                genericConverter.convert(entry.getDiagnostics(), DIAGNOSTICS_MAP_TYPE_REF.getType());
        Metadata diagnostics = Metadata.from(diagnosticsMap != null ? diagnosticsMap : Map.of());
        MessageStream.Entry<M> messageEntry =
                (MessageStream.Entry<M>) converter.convert(entry.getMessage(), eventConverter, genericConverter);
        return new MongoDeadLetter<>(entry, diagnostics, messageEntry.message(), messageEntry);
    }

    @Override
    public CompletableFuture<Boolean> isFull(Object sequenceIdentifier, @Nullable ProcessingContext context) {
        String stringSequenceIdentifier = toStringSequenceIdentifier(sequenceIdentifier);
        return execute(context, session -> isFull(session, stringSequenceIdentifier));
    }

    private boolean isFull(ClientSession session, String sequenceIdentifier) {
        long numberInSequence = sequenceSize(session, sequenceIdentifier);
        return numberInSequence > 0 ? numberInSequence >= maxSequenceSize : amountOfSequences(session) >= maxSequences;
    }

    @Override
    public CompletableFuture<Long> size(@Nullable ProcessingContext context) {
        return execute(context, session -> collection.countDocuments(session, processingGroupFilter(processingGroup)));
    }

    @Override
    public CompletableFuture<Long> sequenceSize(Object sequenceIdentifier,
                                                @Nullable ProcessingContext context) {
        String stringSequenceIdentifier = toStringSequenceIdentifier(sequenceIdentifier);
        return execute(context, session -> sequenceSize(session, stringSequenceIdentifier));
    }

    private String toStringSequenceIdentifier(Object sequenceIdentifier) {
        return sequenceIdentifier instanceof String identifier
                ? identifier
                : Integer.toString(sequenceIdentifier.hashCode());
    }

    private long sequenceSize(ClientSession session, String seqId) {
        return collection.countDocuments(session, processingGroupAndSequenceIdentifierFilter(processingGroup, seqId));
    }

    @Override
    public CompletableFuture<Long> amountOfSequences(@Nullable ProcessingContext context) {
        return execute(context, this::amountOfSequences);
    }

    private long amountOfSequences(ClientSession session) {
        AtomicLong counter = new AtomicLong(0L);
        sequenceIdentifierIterator(session, collection, processingGroup).forEach(i -> counter.incrementAndGet());
        return counter.get();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Optimized override that retrieves available sequences one at a time instead of in batches. Since no
     * {@code sequenceFilter} is applied, every available sequence is a valid candidate, so there is no need to prefetch
     * multiple sequences. This avoids unnecessary conversion of dead letter entries that will never be evaluated.
     *
     * @see #process(Predicate, Function, ProcessingContext)
     */
    @Override
    public CompletableFuture<Boolean> process(
            Function<DeadLetter<? extends M>, CompletableFuture<EnqueueDecision<M>>> processingTask,
            @Nullable ProcessingContext context
    ) {
        return process(1, letter -> true, processingTask, context);
    }

    @Override
    public CompletableFuture<Boolean> process(
            Predicate<DeadLetter<? extends M>> sequenceFilter,
            Function<DeadLetter<? extends M>, CompletableFuture<EnqueueDecision<M>>> processingTask,
            @Nullable ProcessingContext context
    ) {
        return process(10, sequenceFilter, processingTask, context);
    }

    private CompletableFuture<Boolean> process(
            int batchSize,
            Predicate<DeadLetter<? extends M>> sequenceFilter,
            Function<DeadLetter<? extends M>, CompletableFuture<EnqueueDecision<M>>> processingTask,
            @Nullable ProcessingContext context
    ) {
        CompletableFuture<@Nullable MongoDeadLetter<M>> claim =
                execute(context, session -> claimFirstMatchingLetter(session, batchSize, sequenceFilter));
        return claim.thenCompose(claimedLetter -> {
            if (claimedLetter == null) {
                logger.info("No claimable and/or matching dead letters found to process.");
                return CompletableFuture.completedFuture(false);
            }
            return processLetterAndFollowing(claimedLetter, processingTask, context);
        });
    }

    @Nullable
    private MongoDeadLetter<M> claimFirstMatchingLetter(ClientSession session,
                                                        int batchSize,
                                                        Predicate<DeadLetter<? extends M>> sequenceFilter) {
        try (
                MongoCursor<Document> cursor = collection.aggregate(
                                                                 session,
                                                                 firstOfEachSequencePipeline(
                                                                         processingGroup, processingStartedLimit()
                                                                 )
                                                         )
                                                         .allowDiskUse(allowDiskUse)
                                                         .batchSize(batchSize)
                                                         .iterator()
        ) {
            while (cursor.hasNext()) {
                Document id = new Document("_id", cursor.next().get("_id"));
                Document letterDocument = collection.find(session, id).first();
                if (letterDocument == null) {
                    continue;
                }
                MongoDeadLetter<M> letter = toLetter(new DeadLetterEntry(letterDocument));
                if (sequenceFilter.test(letter) && claimDeadLetter(session, letter)) {
                    return letter;
                }
            }
        }
        return null;
    }

    /**
     * Processes the given {@code firstDeadLetter} using the provided {@code processingTask}. When successful (the
     * message is evicted) it will automatically process all messages in the same
     * {@link MongoDeadLetter#sequenceIdentifier()}, evicting messages that succeed and stopping when the first one
     * fails (and is requeued).
     * <p>
     * Will claim the next letter in the same sequence before removing the old one to prevent concurrency issues.
     *
     * @param firstDeadLetter the dead letter to start processing
     * @param processingTask  the task to use to process the dead letter, providing a decision afterward
     * @param context         the {@link ProcessingContext} to resolve the {@link TransactionalExecutor} with, may be
     *                        {@code null}
     * @return whether processing all letters in this sequence was successful
     */
    private CompletableFuture<Boolean> processLetterAndFollowing(
            MongoDeadLetter<M> firstDeadLetter,
            Function<DeadLetter<? extends M>, CompletableFuture<EnqueueDecision<M>>> processingTask,
            @Nullable ProcessingContext context
    ) {
        logger.info("Processing dead letter with sequence identifier [{}] and index [{}]",
                    firstDeadLetter.sequenceIdentifier(), firstDeadLetter.index());

        CompletableFuture<EnqueueDecision<M>> decisionFuture;
        try {
            decisionFuture = processingTask.apply(firstDeadLetter);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        return decisionFuture.thenCompose(decision -> {
            if (decision.shouldEnqueue()) {
                return requeue(
                        firstDeadLetter,
                        l -> decision.withDiagnostics(l)
                                     .withCause(decision.enqueueCause()
                                                        .orElse(null)),
                        context
                ).thenApply(ignored -> false);
            }
            CompletableFuture<@Nullable MongoDeadLetter<M>> nextLetter = execute(context, session -> {
                Document nextLetterDocument = findNextDeadLetter(session, firstDeadLetter);
                if (nextLetterDocument == null) {
                    evictLetter(session, firstDeadLetter);
                    return null;
                }
                MongoDeadLetter<M> nextDeadLetter = toLetter(new DeadLetterEntry(nextLetterDocument));
                claimDeadLetter(session, nextDeadLetter);
                evictLetter(session, firstDeadLetter);
                return nextDeadLetter;
            });
            return nextLetter.thenCompose(nextDeadLetter -> nextDeadLetter == null
                    ? CompletableFuture.completedFuture(true)
                    : processLetterAndFollowing(nextDeadLetter, processingTask, context));
        });
    }

    /**
     * Finds the next dead letter after the provided {@code oldLetter} in the database. This is the message in the same
     * {@code processingGroup} and {@code sequence}, but with the next index.
     *
     * @param session   the {@link ClientSession} to search with
     * @param oldLetter the base letter to search from
     * @return the document of the next letter to process, or {@code null} if there is none
     */
    @Nullable
    private Document findNextDeadLetter(ClientSession session, MongoDeadLetter<M> oldLetter) {
        return collection.find(
                                 session,
                                 nextItemInSequenceFilter(
                                         processingGroup, oldLetter.sequenceIdentifier(), oldLetter.index()
                                 )
                         )
                         .sort(indexSortAscending())
                         .limit(1)
                         .first();
    }

    /**
     * Claims the provided {@link DeadLetter} in the database by setting the {@code processingStarted} property. The
     * update only matches when the letter is not claimed or its claim has expired, making the claim atomic.
     *
     * @param session    the {@link ClientSession} to claim with
     * @param deadLetter the letter to claim
     * @return whether the letter was successfully claimed or not
     */
    private boolean claimDeadLetter(ClientSession session, MongoDeadLetter<M> deadLetter) {
        UpdateResult result = collection.updateOne(
                session,
                uniqueNotLockedFilter(
                        processingGroup, deadLetter.sequenceIdentifier(), deadLetter.index(), processingStartedLimit()
                ),
                updateProcessingStarted(GenericDeadLetter.clock.instant())
        );

        if (result.getMatchedCount() > 0) {
            logger.info("Claimed dead letter with sequence identifier [{}] and index [{}] to process.",
                        deadLetter.sequenceIdentifier(), deadLetter.index());
            return true;
        }

        logger.info("Failed to claim dead letter with sequence identifier [{}] and index [{}].",
                    deadLetter.sequenceIdentifier(), deadLetter.index());
        return false;
    }

    private Instant processingStartedLimit() {
        return GenericDeadLetter.clock.instant().minus(claimDuration);
    }

    private void evictLetter(ClientSession session, MongoDeadLetter<M> deadLetter) {
        if (logger.isInfoEnabled()) {
            logger.info("Trying to evict dead letter with processing group {} sequence {} and index {}",
                        processingGroup, deadLetter.sequenceIdentifier(), deadLetter.index());
        }
        DeleteResult result = collection.deleteOne(
                session,
                findOneFilter(processingGroup, deadLetter.sequenceIdentifier(), deadLetter.index())
        );
        if (logger.isInfoEnabled()) {
            if (result.getDeletedCount() == 1) {
                logger.info("Successfully evict dead letter with processing group {} sequence {} and index {}",
                            processingGroup, deadLetter.sequenceIdentifier(), deadLetter.index());
            } else {
                logger.info(
                        "Failed to evict dead letter with processing group {} sequence {} and index {} with result {}",
                        processingGroup, deadLetter.sequenceIdentifier(), deadLetter.index(), result
                );
            }
        }
    }

    @Override
    public CompletableFuture<Void> clear(@Nullable ProcessingContext context) {
        return accept(
                context,
                session -> collection.deleteMany(session, processingGroupFilter(processingGroup))
        );
    }

    private <T extends @Nullable Object> CompletableFuture<T> execute(
            @Nullable ProcessingContext context,
            ThrowingFunction<ClientSession, T, Exception> action
    ) {
        return FutureUtils.runFailing(() -> executorFor(context).apply(action));
    }

    private CompletableFuture<Void> accept(@Nullable ProcessingContext context,
                                           ThrowingConsumer<ClientSession, Exception> action) {
        return FutureUtils.runFailing(() -> executorFor(context).accept(action));
    }

    private TransactionalExecutor<ClientSession> executorFor(@Nullable ProcessingContext context) {
        return transactionalExecutorProvider.getTransactionalExecutor(context);
    }
}
