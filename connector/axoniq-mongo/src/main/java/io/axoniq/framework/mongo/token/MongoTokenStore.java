/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.token;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import io.axoniq.framework.mongo.MongoAxoniqAddon;
import io.axoniq.framework.mongo.transaction.ClientSessionTransactionManager;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.ClockUtils;
import org.axonframework.common.function.ThrowingFunction;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.ConfigToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.LegacyTokenTypes;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.UnableToClaimTokenException;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.UnableToInitializeTokenException;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.UnableToRetrieveIdentifierException;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static java.lang.String.format;
import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * A {@link TokenStore} that stores {@link TrackingToken TrackingTokens} in MongoDB.
 * <p>
 * Every segment of every processor is stored as a single document in the tracking tokens collection it is constructed
 * with. A document holds the {@code processorName}, {@code segment} and {@code mask} of the segment, the {@code owner}
 * node and the {@code timestamp} (epoch milliseconds) of the last claim, plus the serialized {@code token} bytes and
 * the class name of the token as {@code tokenType}. A unique index on {@code processorName} and {@code segment} is
 * created upon construction, unless {@link MongoTokenStoreConfiguration#ensureIndexes()} is {@code false}.
 * <p>
 * Claiming a token is a single atomic update whose filter only matches a document that is unowned, owned by this node,
 * or whose claim is older than the configured claim timeout. Hence, the claim behavior across nodes is correct without
 * relying on a transaction.
 * <p>
 * Every token operation is executed with the {@link ClientSession} that the {@link TransactionalExecutorProvider}
 * provides for the given {@link ProcessingContext}. When the {@link ClientSessionTransactionManager} is the registered
 * {@code TransactionManager}, that session carries the MongoDB transaction of the processing context. Hence, a token
 * that is stored while handling events is committed or rolled back together with everything else the event handlers
 * write through the same session, such as a projection. This provides exactly-once processing semantics for projections
 * in MongoDB. An operation for which no session is bound to the processing context fails, as it would not be part of
 * that transaction. Note that MongoDB only supports transactions on a replica set or sharded cluster.
 * <p>
 * The storage identifier is the exception, as it is metadata of the store itself instead of the progress of a
 * processor. It is established with a session of its own.
 * <p>
 * Tokens are converted to and from {@code byte[]} using the given {@link Converter}.
 * <p>
 * Constructing this store requires the Axoniq MongoDB entitlement, see {@link MongoAxoniqAddon}.
 * <p>
 * Example usage:
 * <pre>{@code
 * MongoCollection<Document> collection = mongoClient.getDatabase("axon").getCollection("trackingtokens");
 * TokenStore tokenStore = new MongoTokenStore(
 *         collection,
 *         new MongoTransactionalExecutorProvider(mongoClient),
 *         new JacksonConverter(),
 *         MongoTokenStoreConfiguration.DEFAULT.claimTimeout(Duration.ofSeconds(30))
 * );
 * }</pre>
 *
 * @author Joris van der Kallen
 * @author Steven van Beelen
 * @since 3.1.0
 */
public class MongoTokenStore implements TokenStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoTokenStore.class);

    private static final String CONFIG_TOKEN_ID = "__config";
    private static final int CONFIG_SEGMENT = 0;
    private static final String CONFIG_ID_KEY = "id";

    private static final String OWNER_PROPERTY_NAME = "owner";
    private static final String TIMESTAMP_PROPERTY_NAME = "timestamp";
    private static final String PROCESSOR_NAME_PROPERTY_NAME = "processorName";
    private static final String SEGMENT_PROPERTY_NAME = "segment";
    private static final String MASK_PROPERTY_NAME = "mask";
    private static final String TOKEN_PROPERTY_NAME = "token";
    private static final String TOKEN_TYPE_PROPERTY_NAME = "tokenType";

    private final MongoCollection<Document> collection;
    private final TransactionalExecutorProvider<ClientSession> transactionalExecutorProvider;
    private final Converter converter;
    private final TemporalAmount claimTimeout;
    private final String nodeId;

    /**
     * Constructs a {@code MongoTokenStore}. Registers the Axoniq MongoDB token store addon with the
     * {@link EntitlementManager}, and creates the unique index when so configured.
     *
     * @param tokenStoreCollection          the {@link MongoCollection} storing the tokens
     * @param transactionalExecutorProvider the {@link TransactionalExecutorProvider} providing the
     *                                      {@link ClientSession} of the {@link ProcessingContext} to execute every
     *                                      token operation with, so they take part in the MongoDB transaction of that
     *                                      context
     * @param converter                     the {@link Converter} used to convert tokens to and from {@code byte[]}
     * @param configuration                 the {@link MongoTokenStoreConfiguration} holding claim timeout, node
     *                                      identifier and index creation behavior
     */
    public MongoTokenStore(MongoCollection<Document> tokenStoreCollection,
                           TransactionalExecutorProvider<ClientSession> transactionalExecutorProvider,
                           Converter converter,
                           MongoTokenStoreConfiguration configuration) {
        assertNonNull(tokenStoreCollection,
                      "The tracking tokens collection is a hard requirement and should be provided.");
        assertNonNull(transactionalExecutorProvider,
                      "The TransactionalExecutorProvider is a hard requirement and should be provided.");
        assertNonNull(converter, "The Converter is a hard requirement and should be provided.");

        EntitlementManager.INSTANCE.registerAddon(MongoAxoniqAddon.class);

        this.collection = tokenStoreCollection;
        this.transactionalExecutorProvider = transactionalExecutorProvider;
        this.converter = converter;
        this.claimTimeout = configuration.claimTimeout();
        this.nodeId = configuration.nodeId();

        if (configuration.ensureIndexes()) {
            createIndexes();
        }
    }

    /**
     * Creates the unique index on {@code processorName} and {@code segment} required by this store. Invoked from the
     * constructor unless {@link MongoTokenStoreConfiguration#ensureIndexes()} is {@code false}; creating an index that
     * already exists is a no-op. The index is created with a session of its own, as obtained from the
     * {@link TransactionalExecutorProvider} without a {@link ProcessingContext}, so it never takes part in a
     * transaction.
     */
    public void ensureIndexes() {
        createIndexes();
    }

    private void createIndexes() {
        // Index creation is schema management, not progress of a processor. Like the storage identifier, it runs with
        // a session of its own, outside of any processing context.
        execute(
                transactionalExecutorProvider.getTransactionalExecutor(null),
                session -> collection.createIndex(
                        session,
                        Indexes.ascending(PROCESSOR_NAME_PROPERTY_NAME, SEGMENT_PROPERTY_NAME),
                        new IndexOptions().unique(true)
                )
        ).orTimeout(30, TimeUnit.SECONDS).join();
    }

    @Override
    public CompletableFuture<List<Segment>> initializeTokenSegments(String processorName,
                                                                    int segmentCount,
                                                                    @Nullable TrackingToken initialToken,
                                                                    ProcessingContext context) {
        return fetchSegments(processorName, context).thenCompose(existing -> {
            if (!existing.isEmpty()) {
                return CompletableFuture.failedFuture(new UnableToClaimTokenException(
                        "Could not initialize segments. Some segments were already present."));
            }
            return execute(context, session -> {
                List<Segment> segments = Segment.splitBalanced(Segment.ROOT_SEGMENT, segmentCount - 1);
                List<Document> documents = new ArrayList<>(segments.size());
                for (Segment segment : segments) {
                    documents.add(newDocument(initialToken, processorName, segment));
                }
                try {
                    collection.insertMany(session, documents, new InsertManyOptions().ordered(false));
                } catch (MongoBulkWriteException e) {
                    throw new UnableToClaimTokenException(
                            "Could not initialize segments. Some segments were already present.", e);
                }
                return segments;
            });
        });
    }

    @Override
    public CompletableFuture<Void> storeToken(@Nullable TrackingToken token,
                                              String processorName,
                                              int segmentId,
                                              ProcessingContext context) {
        return execute(context, session -> {
            byte[] data = token == null ? null : converter.convert(token, byte[].class);
            String type = token == null ? null : token.getClass().getName();
            UpdateResult result = collection.updateOne(
                    session,
                    and(segmentFilter(processorName, segmentId), claimableCondition()),
                    combine(
                            combine(
                                    set(OWNER_PROPERTY_NAME, nodeId),
                                    set(TIMESTAMP_PROPERTY_NAME, ClockUtils.instant().toEpochMilli())
                            ),
                            set(TOKEN_PROPERTY_NAME, data),
                            set(TOKEN_TYPE_PROPERTY_NAME, type)
                    )
            );
            if (result.getMatchedCount() == 0) {
                throw claimFailure(session, processorName, segmentId);
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                       int segmentId,
                                                       ProcessingContext context) {
        return execute(context, session -> claim(session, processorName, segmentId));
    }

    @Override
    public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                       Segment segment,
                                                       ProcessingContext context) {
        return execute(context, session -> {
            if (collection.countDocuments(session, segmentFilter(processorName, segment.getSegmentId())) > 0) {
                validateSegment(session, processorName, segment);
            }
            return claim(session, processorName, segment.getSegmentId());
        });
    }

    private TrackingToken claim(ClientSession session, String processorName, int segmentId) {
        Document document = collection.findOneAndUpdate(
                session,
                and(segmentFilter(processorName, segmentId), claimableCondition()),
                combine(
                        set(OWNER_PROPERTY_NAME, nodeId),
                        set(TIMESTAMP_PROPERTY_NAME, ClockUtils.instant().toEpochMilli())
                ),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
        );
        if (document == null) {
            throw claimFailure(session, processorName, segmentId);
        }
        return readToken(document);
    }

    /**
     * Builds the failure for an unsuccessful claim attempt by checking whether the token exists at all.
     */
    private UnableToClaimTokenException claimFailure(ClientSession session, String processorName, int segmentId) {
        Document existing = collection.find(session, segmentFilter(processorName, segmentId)).first();
        if (existing == null) {
            return new UnableToClaimTokenException(format(
                    "Unable to claim token '%s[%s]'. It has not been initialized yet", processorName, segmentId
            ));
        }
        return new UnableToClaimTokenException(format(
                "Unable to claim token '%s[%s]'. It is owned by '%s'",
                processorName, segmentId, existing.getString(OWNER_PROPERTY_NAME)
        ));
    }

    private void validateSegment(ClientSession session, String processorName, Segment segment) {
        // The split segment should not exist, the mergeable segment should exist.
        long count = collection.countDocuments(session, and(
                eq(PROCESSOR_NAME_PROPERTY_NAME, processorName),
                in(SEGMENT_PROPERTY_NAME, segment.splitSegmentId(), segment.mergeableSegmentId())
        ));
        if (count == 0) {
            throw new UnableToClaimTokenException(format(
                    "Unable to claim token '%s[%s]'. It has been merged with another segment",
                    processorName, segment.getSegmentId()
            ));
        }
        if (count >= 2) {
            throw new UnableToClaimTokenException(format(
                    "Unable to claim token '%s[%s]'. It has been split into two segments",
                    processorName, segment.getSegmentId()
            ));
        }
    }

    @Override
    public CompletableFuture<Void> extendClaim(String processorName,
                                               int segmentId,
                                               ProcessingContext context) {
        return execute(context, session -> {
            UpdateResult result = collection.updateOne(
                    session,
                    and(segmentFilter(processorName, segmentId), eq(OWNER_PROPERTY_NAME, nodeId)),
                    set(TIMESTAMP_PROPERTY_NAME, ClockUtils.instant().toEpochMilli())
            );
            if (result.getMatchedCount() == 0) {
                throw new UnableToClaimTokenException(format(
                        "Unable to extend claim on token '%s[%s]'. It is not owned by '%s'",
                        processorName, segmentId, nodeId
                ));
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> releaseClaim(String processorName, int segmentId, ProcessingContext context) {
        return execute(context, session -> {
            UpdateResult result = collection.updateOne(
                    session,
                    and(segmentFilter(processorName, segmentId), eq(OWNER_PROPERTY_NAME, nodeId)),
                    combine(set(OWNER_PROPERTY_NAME, null), set(TIMESTAMP_PROPERTY_NAME,
                                                                ClockUtils.instant().toEpochMilli()))
            );
            if (result.getMatchedCount() == 0) {
                logger.warn("Releasing claim of token {}/{} failed. It was owned by another node.",
                            processorName, segmentId);
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> initializeSegment(@Nullable TrackingToken token,
                                                     String processorName,
                                                     Segment segment,
                                                     ProcessingContext context) {
        return execute(context, session -> {
            try {
                collection.insertOne(session, newDocument(token, processorName, segment));
            } catch (MongoWriteException e) {
                if (isDuplicateKey(e)) {
                    throw new UnableToInitializeTokenException(
                            format("Unable to initialize token '%s[%s]'", processorName, segment.getSegmentId()), e
                    );
                }
                throw e;
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> deleteToken(String processorName, int segmentId, ProcessingContext context) {
        return execute(context, session -> {
            DeleteResult result = collection.deleteOne(
                    session,
                    and(segmentFilter(processorName, segmentId), eq(OWNER_PROPERTY_NAME, nodeId))
            );
            if (result.getDeletedCount() == 0) {
                throw new UnableToClaimTokenException("Unable to remove token. It is not owned by " + nodeId);
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Segment> fetchSegment(String processorName, int segmentId, ProcessingContext context) {
        return execute(context, session -> {
            Document document = collection.find(session, segmentFilter(processorName, segmentId)).first();
            return document == null ? null : toSegment(document);
        });
    }

    @Override
    public CompletableFuture<List<Segment>> fetchSegments(String processorName, ProcessingContext context) {
        return execute(context, session -> collection.find(session, eq(PROCESSOR_NAME_PROPERTY_NAME, processorName))
                                                     .sort(ascending(SEGMENT_PROPERTY_NAME))
                                                     .map(MongoTokenStore::toSegment)
                                                     .into(new ArrayList<>()));
    }

    @Override
    public CompletableFuture<List<Segment>> fetchAvailableSegments(String processorName, ProcessingContext context) {
        return execute(context,
                       session -> collection.find(session,
                                                  and(eq(PROCESSOR_NAME_PROPERTY_NAME, processorName),
                                                      claimableCondition())
                                            )
                                            .sort(ascending(SEGMENT_PROPERTY_NAME))
                                            .map(MongoTokenStore::toSegment)
                                            .into(new ArrayList<>())
        );
    }

    private <T> CompletableFuture<T> execute(ProcessingContext context,
                                             ThrowingFunction<ClientSession, T, Exception> action) {
        TransactionalExecutor<ClientSession> executor =
                transactionalExecutorProvider.findTransactionalExecutor(context);
        if (executor == null) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "No MongoDB transaction is bound to the processing context. Register a "
                            + ClientSessionTransactionManager.class.getSimpleName()
                            + " (in Spring: a SpringMongoTransactionManager) as the TransactionManager, so token "
                            + "operations take part in the transaction of the event handling that they belong to. "
                            + "Note that only one TransactionManager serves an application."
            ));
        }
        return execute(executor, action);
    }

    private Bson claimableCondition() {
        return or(
                eq(OWNER_PROPERTY_NAME, nodeId),
                eq(OWNER_PROPERTY_NAME, null),
                lt(TIMESTAMP_PROPERTY_NAME, ClockUtils.instant().minus(claimTimeout).toEpochMilli())
        );
    }

    private static Segment toSegment(Document document) {
        return new Segment(
                document.getInteger(SEGMENT_PROPERTY_NAME),
                document.getInteger(MASK_PROPERTY_NAME)
        );
    }

    @Override
    public CompletableFuture<String> retrieveStorageIdentifier(ProcessingContext context) {
        // The storage identifier is metadata about the store itself, not progress of a processor. It is established
        // with a session of its own, so a concurrent initialization (which surfaces as a duplicate key) cannot abort
        // the transaction of the processing context.
        return execute(transactionalExecutorProvider.getTransactionalExecutor(null), session -> {
            try {
                Document document = collection.find(session, segmentFilter(CONFIG_TOKEN_ID, CONFIG_SEGMENT))
                                              .first();
                if (document == null) {
                    document = initializeConfigDocument(session);
                }
                return ((ConfigToken) readToken(document)).get(CONFIG_ID_KEY);
            } catch (Exception e) {
                throw new UnableToRetrieveIdentifierException(
                        "Exception occurred while trying to establish storage identifier", e
                );
            }
        });
    }

    private static <T> CompletableFuture<T> execute(TransactionalExecutor<ClientSession> executor,
                                                    ThrowingFunction<ClientSession, T, Exception> action) {
        return executor.apply(action).exceptionallyCompose(failure -> {
            Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                    ? failure.getCause()
                    : failure;
            if (cause instanceof MongoException mongoException
                    && mongoException.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                return CompletableFuture.failedFuture(new UnableToClaimTokenException(
                        "The token was concurrently modified by another transaction, most likely a claim by "
                                + "another node.", mongoException
                ));
            }
            return CompletableFuture.failedFuture(cause);
        });
    }

    private Document initializeConfigDocument(ClientSession session) {
        ConfigToken configToken = new ConfigToken(
                Collections.singletonMap(CONFIG_ID_KEY, UUID.randomUUID().toString())
        );
        Document document = newDocument(configToken, CONFIG_TOKEN_ID, new Segment(CONFIG_SEGMENT, 0));
        try {
            collection.insertOne(session, document);
        } catch (MongoWriteException e) {
            if (!isDuplicateKey(e)) {
                throw e;
            }
            // Another node initialized the config concurrently: use its identifier.
        }
        return collection.find(
                session,
                segmentFilter(CONFIG_TOKEN_ID, CONFIG_SEGMENT)
        ).first();
    }

    private Document newDocument(@Nullable TrackingToken token, String processorName, Segment segment) {
        return new Document(PROCESSOR_NAME_PROPERTY_NAME, processorName)
                .append(SEGMENT_PROPERTY_NAME, segment.getSegmentId())
                .append(MASK_PROPERTY_NAME, segment.getMask())
                .append(OWNER_PROPERTY_NAME, null)
                .append(TIMESTAMP_PROPERTY_NAME, ClockUtils.instant().toEpochMilli())
                .append(TOKEN_PROPERTY_NAME, token == null ? null : converter.convert(token, byte[].class))
                .append(TOKEN_TYPE_PROPERTY_NAME, token == null ? null : token.getClass().getName());
    }

    private static boolean isDuplicateKey(MongoWriteException exception) {
        return ErrorCategory.fromErrorCode(exception.getError().getCode()) == ErrorCategory.DUPLICATE_KEY;
    }

    private static Bson segmentFilter(String processorName, int segmentId) {
        return and(eq(PROCESSOR_NAME_PROPERTY_NAME, processorName), eq(SEGMENT_PROPERTY_NAME, segmentId));
    }

    @Nullable
    private TrackingToken readToken(Document document) {
        Binary data = document.get(TOKEN_PROPERTY_NAME, Binary.class);
        String type = document.getString(TOKEN_TYPE_PROPERTY_NAME);
        if (data == null || type == null) {
            return null;
        }
        return LegacyTokenTypes.deserialize(converter, data.getData(), type);
    }
}
