/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.token;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import io.axoniq.framework.mongo.MongoAxoniqAddon;
import io.axoniq.framework.mongo.transaction.MongoSessionContexts;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import io.axoniq.framework.mongo.util.MongoMocks;
import io.axoniq.license.entitlement.AxoniqAddonNotGrantedException;
import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.license.entitlement.EntitlementMessageType;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.UnableToClaimTokenException;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.UnableToInitializeTokenException;
import org.bson.BsonDocument;
import org.bson.types.Binary;
import org.bson.BsonDocumentReader;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class validating how the {@link MongoTokenStore} uses its collection and the session of the processing
 * context. The behavior of the store against a real MongoDB instance is validated by the
 * {@code MongoTokenStoreEventProcessingIT}.
 *
 * @author Joris van der Kallen
 * @author Steven van Beelen
 */
class MongoTokenStoreTest {

    private static final String PROCESSOR = "testProcessorName";
    private static final String OWNER = "testOwner";

    private final Converter converter = new JacksonConverter();
    private ClientSession session;
    private ProcessingContext context;
    private MongoTransactionalExecutorProvider provider;
    private MongoCollection<Document> collection;
    private MongoTokenStore testSubject;

    @BeforeEach
    void setUp() {
        session = mock(ClientSession.class);
        MongoClient mongoClient = mock(MongoClient.class);
        when(mongoClient.startSession()).thenReturn(session);
        provider = new MongoTransactionalExecutorProvider(mongoClient);
        context = MongoSessionContexts.withSession(mongoClient);
        collection = MongoMocks.collection();
        testSubject = new MongoTokenStore(collection,
                                          provider,
                                          converter,
                                          MongoTokenStoreConfiguration.DEFAULT.nodeId(OWNER).ensureIndexes(false));
    }

    @Nested
    class Construction {

        @Test
        void registersTheMongoAddonWithTheEntitlementManager() {
            // given / when
            new MongoTokenStore(collection, provider, converter, MongoTokenStoreConfiguration.DEFAULT);

            // then
            try {
                EntitlementManager.INSTANCE.claimMessage(MongoAxoniqAddon.IDENTIFIER,
                                                         EntitlementMessageType.EVENT,
                                                         1);
            } catch (AxoniqAddonNotGrantedException e) {
                assertThat(e.getMessage())
                        .as("Mongo addon should be registered with the entitlement system")
                        .doesNotContain("Addon was not found in the application");
            }
        }

        @Test
        void rejectsMissingDependencies() {
            MongoTokenStoreConfiguration configuration = MongoTokenStoreConfiguration.DEFAULT;

            assertThatThrownBy(() -> new MongoTokenStore(null, provider, converter, configuration))
                    .isInstanceOf(AxonConfigurationException.class);
            assertThatThrownBy(() -> new MongoTokenStore(collection, null, converter, configuration))
                    .isInstanceOf(AxonConfigurationException.class);
            assertThatThrownBy(() -> new MongoTokenStore(collection, provider, null, configuration))
                    .isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void rejectsEmptyNodeId() {
            assertThatThrownBy(() -> MongoTokenStoreConfiguration.DEFAULT.nodeId(""))
                    .isInstanceOf(AxonConfigurationException.class);
        }
    }

    @Nested
    class WithoutAMongoDbSessionOnTheProcessingContext {

        @Test
        void operationsFailAsTheyWouldNotTakePartInTheTransaction() {
            // given
            ProcessingContext contextWithoutSession = new StubProcessingContext();

            // when / then
            assertThatThrownBy(() -> join(testSubject.fetchToken(PROCESSOR, 0, contextWithoutSession)))
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("ClientSessionTransactionManager");
        }
    }

    @Nested
    class ReadingTokens {

        @Test
        void readsATokenThatAxonFramework4StoredUnderItsOldClassName() throws Exception {
            // given
            Document stored = new Document("processorName", PROCESSOR)
                    .append("segment", 0)
                    .append("mask", 0)
                    .append("token", new Binary("{\"globalIndex\":5}".getBytes(StandardCharsets.UTF_8)))
                    .append("tokenType", "org.axonframework.eventhandling.GlobalSequenceTrackingToken");
            when(collection.findOneAndUpdate(any(ClientSession.class),
                                             any(Bson.class),
                                             any(Bson.class),
                                             any(FindOneAndUpdateOptions.class))).thenReturn(stored);

            // when
            TrackingToken result = join(testSubject.fetchToken(PROCESSOR, 0, context));

            // then
            assertThat(result).isEqualTo(new GlobalSequenceTrackingToken(5));
        }
    }

    @Nested
    class StoringTokens {

        @Test
        void updatesTheTokenWithTheSessionOfTheProcessingContext() throws Exception {
            // given
            when(collection.updateOne(any(ClientSession.class), any(Bson.class), any(Bson.class)))
                    .thenReturn(UpdateResult.acknowledged(1, 1L, null));
            GlobalSequenceTrackingToken token = new GlobalSequenceTrackingToken(42);

            // when
            join(testSubject.storeToken(token, PROCESSOR, 0, context));

            // then
            ArgumentCaptor<Bson> update = ArgumentCaptor.forClass(Bson.class);
            verify(collection).updateOne(eq(session), any(Bson.class), update.capture());
            BsonDocument set = render(update.getValue()).getDocument("$set");
            assertThat(set.getString("owner").getValue()).isEqualTo(OWNER);
            assertThat(set.getString("tokenType").getValue()).isEqualTo(GlobalSequenceTrackingToken.class.getName());
            assertThat(set.get("token").isBinary()).isTrue();
        }

        @Test
        void storingANullTokenClearsTheTokenAndItsType() throws Exception {
            // given
            when(collection.updateOne(any(ClientSession.class), any(Bson.class), any(Bson.class)))
                    .thenReturn(UpdateResult.acknowledged(1, 1L, null));

            // when
            join(testSubject.storeToken(null, PROCESSOR, 0, context));

            // then
            ArgumentCaptor<Bson> update = ArgumentCaptor.forClass(Bson.class);
            verify(collection).updateOne(eq(session), any(Bson.class), update.capture());
            BsonDocument set = render(update.getValue()).getDocument("$set");
            assertThat(set.get("token").isNull()).isTrue();
            assertThat(set.get("tokenType").isNull()).isTrue();
        }

        @Test
        void failsWhenTheTokenCannotBeClaimedBecauseTheSegmentWasNotInitialized() {
            // given
            when(collection.updateOne(any(ClientSession.class), any(Bson.class), any(Bson.class)))
                    .thenReturn(UpdateResult.acknowledged(0, 0L, null));
            FindIterable<Document> existing = findingNothing();
            when(collection.find(any(ClientSession.class), any(Bson.class))).thenReturn(existing);

            // when / then
            assertThatThrownBy(() -> join(testSubject.storeToken(null, PROCESSOR, 0, context)))
                    .hasCauseInstanceOf(UnableToClaimTokenException.class)
                    .hasMessageContaining("has not been initialized yet");
        }

        @Test
        void failsWhenTheTokenIsOwnedByAnotherNode() {
            // given
            when(collection.updateOne(any(ClientSession.class), any(Bson.class), any(Bson.class)))
                    .thenReturn(UpdateResult.acknowledged(0, 0L, null));
            @SuppressWarnings("unchecked")
            FindIterable<Document> existing = mock(FindIterable.class);
            when(existing.first()).thenReturn(new Document("owner", "anotherOwner"));
            when(collection.find(any(ClientSession.class), any(Bson.class))).thenReturn(existing);

            // when / then
            assertThatThrownBy(() -> join(testSubject.storeToken(null, PROCESSOR, 0, context)))
                    .hasCauseInstanceOf(UnableToClaimTokenException.class)
                    .hasMessageContaining("owned by 'anotherOwner'");
        }

        @Test
        void aConcurrentlyModifiedTokenFailsAsAClaimFailure() {
            // given
            MongoException transientFailure = new MongoException("Conflicting write");
            transientFailure.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
            when(collection.updateOne(any(ClientSession.class), any(Bson.class), any(Bson.class)))
                    .thenThrow(transientFailure);

            // when / then
            assertThatThrownBy(() -> join(testSubject.storeToken(null, PROCESSOR, 0, context)))
                    .hasCauseInstanceOf(UnableToClaimTokenException.class)
                    .hasMessageContaining("concurrently modified");
        }
    }

    @Nested
    class InitializingSegments {

        @Test
        void insertsTheTokenOfTheSegmentWithTheSessionOfTheProcessingContext() throws Exception {
            // given
            Segment segment = new Segment(3, 7);

            // when
            join(testSubject.initializeSegment(new GlobalSequenceTrackingToken(5), PROCESSOR, segment, context));

            // then
            ArgumentCaptor<Document> inserted = ArgumentCaptor.forClass(Document.class);
            verify(collection).insertOne(eq(session), inserted.capture());
            assertThat(inserted.getValue()).containsEntry("processorName", PROCESSOR)
                                           .containsEntry("segment", 3)
                                           .containsEntry("mask", 7)
                                           .containsEntry("owner", null)
                                           .containsEntry("tokenType", GlobalSequenceTrackingToken.class.getName());
        }

        @Test
        void failsWhenTheSegmentIsAlreadyPresent() {
            // given
            WriteError duplicateKey = new WriteError(11000, "duplicate key", new BsonDocument());
            when(collection.insertOne(any(ClientSession.class), any(Document.class)))
                    .thenThrow(new MongoWriteException(duplicateKey, new ServerAddress()));

            // when / then
            assertThatThrownBy(() -> join(testSubject.initializeSegment(null, PROCESSOR, Segment.ROOT_SEGMENT, context)))
                    .hasCauseInstanceOf(UnableToInitializeTokenException.class);
        }
    }

    @Nested
    class DeletingTokens {

        @Test
        void removesTheTokenOwnedByThisNode() throws Exception {
            // given
            when(collection.deleteOne(any(ClientSession.class), any(Bson.class)))
                    .thenReturn(DeleteResult.acknowledged(1L));

            // when
            join(testSubject.deleteToken(PROCESSOR, 0, context));

            // then
            ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
            verify(collection).deleteOne(eq(session), filter.capture());
            assertThat(render(filter.getValue()).toJson()).contains(OWNER).contains(PROCESSOR);
        }

        @Test
        void failsWhenTheTokenIsNotOwnedByThisNode() {
            // given
            when(collection.deleteOne(any(ClientSession.class), any(Bson.class)))
                    .thenReturn(DeleteResult.acknowledged(0L));

            // when / then
            assertThatThrownBy(() -> join(testSubject.deleteToken(PROCESSOR, 0, context)))
                    .hasCauseInstanceOf(UnableToClaimTokenException.class)
                    .hasMessageContaining("not owned by " + OWNER);
        }
    }

    @SuppressWarnings("unchecked")
    private FindIterable<Document> findingNothing() {
        FindIterable<Document> iterable = mock(FindIterable.class);
        when(iterable.first()).thenReturn(null);
        return iterable;
    }

    private static BsonDocument render(Bson bson) {
        return bson.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
    }

    private static <T> T join(CompletableFuture<T> future) throws Exception {
        return future.get(10, TimeUnit.SECONDS);
    }
}
