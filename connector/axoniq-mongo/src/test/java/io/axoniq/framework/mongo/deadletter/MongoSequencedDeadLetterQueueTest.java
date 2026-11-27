/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.DeadLetterQueueOverflowException;
import io.axoniq.framework.messaging.deadletter.GenericDeadLetter;
import io.axoniq.framework.messaging.deadletter.NoSuchDeadLetterException;
import io.axoniq.framework.messaging.deadletter.WrongDeadLetterTypeException;
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
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class validating the {@link MongoSequencedDeadLetterQueue} and its
 * {@link MongoSequencedDeadLetterQueueConfiguration}: the construction checks, the registration of the addon, and how
 * the queue uses its collection and the session of the processing context. The behavior of the queue against a real
 * MongoDB instance is validated by the {@link MongoDeadLetteringIT}.
 *
 * @author Steven van Beelen
 */
class MongoSequencedDeadLetterQueueTest {

    private static final String PROCESSING_GROUP = "testGroup";
    private static final String SEQUENCE = "sequence";

    private final JacksonConverter jacksonConverter = new JacksonConverter();
    private final EventConverter eventConverter = new DelegatingEventConverter(jacksonConverter);
    private ClientSession session;
    private ProcessingContext context;
    private MongoTransactionalExecutorProvider provider;
    private MongoCollection<Document> collection;

    @BeforeEach
    void setUp() {
        session = mock(ClientSession.class);
        MongoClient mongoClient = mock(MongoClient.class);
        when(mongoClient.startSession()).thenReturn(session);
        provider = new MongoTransactionalExecutorProvider(mongoClient);
        context = MongoSessionContexts.withSession(mongoClient);
        collection = MongoMocks.collection();
    }

    private MongoSequencedDeadLetterQueue<EventMessage> construct(String processingGroup,
                                                                  MongoCollection<Document> collection,
                                                                  TransactionalExecutorProvider<ClientSession> provider,
                                                                  EventConverter eventConverter,
                                                                  Converter genericConverter,
                                                                  MongoSequencedDeadLetterQueueConfiguration config) {
        return new MongoSequencedDeadLetterQueue<>(processingGroup,
                                                   collection,
                                                   provider,
                                                   eventConverter,
                                                   genericConverter,
                                                   config);
    }

    private MongoSequencedDeadLetterQueue<EventMessage> queue(MongoSequencedDeadLetterQueueConfiguration config) {
        return new MongoSequencedDeadLetterQueue<>(PROCESSING_GROUP,
                                                   collection,
                                                   provider,
                                                   eventConverter,
                                                   jacksonConverter,
                                                   config.ensureIndexes(false));
    }

    private MongoSequencedDeadLetterQueue<EventMessage> queue() {
        return queue(MongoSequencedDeadLetterQueueConfiguration.DEFAULT);
    }

    private static DeadLetter<EventMessage> letter() {
        return new GenericDeadLetter<EventMessage>(SEQUENCE, EventTestUtils.asEventMessage("payload"))
                .withCause(new IllegalStateException("boom"));
    }

    private static MongoDeadLetter<EventMessage> storedLetter(long index) {
        Instant now = Instant.now();
        return new MongoDeadLetter<>(index,
                                     SEQUENCE,
                                     now,
                                     now,
                                     null,
                                     Metadata.emptyInstance(),
                                     EventTestUtils.asEventMessage("payload"),
                                     Context.empty());
    }

    private void sequenceHasLetters(long count) {
        when(collection.countDocuments(any(ClientSession.class), any(Bson.class))).thenReturn(count);
    }

    @SuppressWarnings("unchecked")
    private void sequencesPresent(String... identifiers) {
        DistinctIterable<String> distinct = mock(DistinctIterable.class);
        doAnswer(invocation -> {
            Consumer<String> consumer = invocation.getArgument(0);
            List.of(identifiers).forEach(consumer);
            return null;
        }).when(distinct).forEach(any(Consumer.class));
        when(collection.distinct(any(ClientSession.class), anyString(), any(Bson.class), eq(String.class)))
                .thenReturn(distinct);
    }

    @SuppressWarnings("unchecked")
    private void highestIndexOfSequenceIs(Document highest) {
        FindIterable<Document> find = mock(FindIterable.class);
        when(find.sort(any(Bson.class))).thenReturn(find);
        when(find.limit(anyInt())).thenReturn(find);
        when(find.first()).thenReturn(highest);
        when(collection.find(any(ClientSession.class), any(Bson.class))).thenReturn(find);
    }

    private static BsonDocument render(Bson bson) {
        return bson.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
    }

    private static <T> T join(CompletableFuture<T> future) throws Exception {
        return future.get(10, TimeUnit.SECONDS);
    }

    @Nested
    class Enqueueing {

        @Test
        void insertsTheLetterWithTheSessionOfTheProcessingContext() throws Exception {
            // given
            sequenceHasLetters(0);
            sequencesPresent();
            highestIndexOfSequenceIs(null);

            // when
            join(queue().enqueue(SEQUENCE, letter(), context));

            // then
            ArgumentCaptor<Document> inserted = ArgumentCaptor.forClass(Document.class);
            verify(collection).insertOne(eq(session), inserted.capture());
            assertThat(inserted.getValue()).containsEntry("processingGroup", PROCESSING_GROUP)
                                           .containsEntry("sequenceIdentifier", SEQUENCE)
                                           .containsEntry("index", 0L);
        }

        @Test
        void usesTheIndexFollowingTheHighestIndexOfTheSequence() throws Exception {
            // given
            sequenceHasLetters(5);
            highestIndexOfSequenceIs(new Document("index", 4L));

            // when
            join(queue().enqueue(SEQUENCE, letter(), context));

            // then
            ArgumentCaptor<Document> inserted = ArgumentCaptor.forClass(Document.class);
            verify(collection).insertOne(eq(session), inserted.capture());
            assertThat(inserted.getValue()).containsEntry("index", 5L);
        }

        @Test
        void failsWhenTheSequenceIsFull() {
            // given
            sequenceHasLetters(2);

            // when / then
            MongoSequencedDeadLetterQueue<EventMessage> testSubject =
                    queue(MongoSequencedDeadLetterQueueConfiguration.DEFAULT.maxSequenceSize(2));
            assertThatThrownBy(() -> join(testSubject.enqueue(SEQUENCE, letter(), context)))
                    .hasCauseInstanceOf(DeadLetterQueueOverflowException.class);
            verify(collection, never()).insertOne(any(ClientSession.class), any(Document.class));
        }

        @Test
        void failsWhenTheMaximumNumberOfSequencesIsReachedAndTheSequenceIsNew() {
            // given
            sequenceHasLetters(0);
            sequencesPresent("other");

            // when / then
            MongoSequencedDeadLetterQueue<EventMessage> testSubject =
                    queue(MongoSequencedDeadLetterQueueConfiguration.DEFAULT.maxSequences(1));
            assertThatThrownBy(() -> join(testSubject.enqueue(SEQUENCE, letter(), context)))
                    .hasCauseInstanceOf(DeadLetterQueueOverflowException.class);
            verify(collection, never()).insertOne(any(ClientSession.class), any(Document.class));
        }

        @Test
        void failsWhenNoMongoDbSessionIsBoundToTheProcessingContext() {
            // given
            ProcessingContext contextWithoutSession = new StubProcessingContext();

            // when / then
            assertThatThrownBy(() -> join(queue().enqueue(SEQUENCE, letter(), contextWithoutSession)))
                    .hasCauseInstanceOf(IllegalStateException.class);
            verify(collection, never()).insertOne(any(ClientSession.class), any(Document.class));
        }
    }

    @Nested
    class Evicting {

        @Test
        void failsForALetterThatDoesNotComeFromMongoDb() {
            // given
            DeadLetter<EventMessage> foreign = letter();

            // when / then
            assertThatThrownBy(() -> join(queue().evict(foreign, context)))
                    .hasCauseInstanceOf(WrongDeadLetterTypeException.class);
        }

        @Test
        void deletesTheLetterWithTheSessionOfTheProcessingContext() throws Exception {
            // given
            when(collection.deleteOne(any(ClientSession.class), any(Bson.class)))
                    .thenReturn(DeleteResult.acknowledged(1L));

            // when
            join(queue().evict(storedLetter(3), context));

            // then
            ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
            verify(collection).deleteOne(eq(session), filter.capture());
            BsonDocument rendered = render(filter.getValue());
            assertThat(rendered.toJson()).contains(PROCESSING_GROUP).contains(SEQUENCE);
            assertThat(rendered.toJson()).contains("\"index\"");
        }
    }

    @Nested
    class Requeueing {

        @Test
        void failsForALetterThatDoesNotComeFromMongoDb() {
            // given
            DeadLetter<EventMessage> foreign = letter();

            // when / then
            assertThatThrownBy(() -> join(queue().requeue(foreign, l -> l, context)))
                    .hasCauseInstanceOf(WrongDeadLetterTypeException.class);
        }

        @Test
        void updatesTheCauseAndDiagnosticsAndReleasesTheClaim() throws Exception {
            // given
            when(collection.updateOne(any(ClientSession.class), any(Bson.class), any(Bson.class)))
                    .thenReturn(UpdateResult.acknowledged(1, 1L, null));

            // when
            join(queue().requeue(storedLetter(3),
                                 l -> l.withCause(new IllegalArgumentException("again"))
                                       .withDiagnostics(Metadata.with("key", "value")),
                                 context));

            // then
            ArgumentCaptor<Bson> update = ArgumentCaptor.forClass(Bson.class);
            verify(collection).updateOne(eq(session), any(Bson.class), update.capture());
            BsonDocument set = render(update.getValue()).getDocument("$set");
            assertThat(set.getString("causeType").getValue()).isEqualTo(IllegalArgumentException.class.getName());
            assertThat(set.getString("causeMessage").getValue()).isEqualTo("again");
            assertThat(set.get("processingStarted").isNull()).isTrue();
        }

        @Test
        void clearsTheCauseWhenTheUpdatedLetterHasNone() throws Exception {
            // given
            when(collection.updateOne(any(ClientSession.class), any(Bson.class), any(Bson.class)))
                    .thenReturn(UpdateResult.acknowledged(1, 1L, null));

            // when
            join(queue().requeue(storedLetter(3), l -> l, context));

            // then
            ArgumentCaptor<Bson> update = ArgumentCaptor.forClass(Bson.class);
            verify(collection).updateOne(eq(session), any(Bson.class), update.capture());
            BsonDocument set = render(update.getValue()).getDocument("$set");
            assertThat(set.get("causeType").isNull()).isTrue();
            assertThat(set.get("causeMessage").isNull()).isTrue();
        }

        @Test
        void failsWhenTheLetterIsNoLongerInTheQueue() {
            // given
            when(collection.updateOne(any(ClientSession.class), any(Bson.class), any(Bson.class)))
                    .thenReturn(UpdateResult.acknowledged(0, 0L, null));

            // when / then
            assertThatThrownBy(() -> join(queue().requeue(storedLetter(3), l -> l, context)))
                    .hasCauseInstanceOf(NoSuchDeadLetterException.class);
        }
    }

    @Nested
    class Inspecting {

        @Test
        void sizeCountsTheLettersOfTheProcessingGroup() throws Exception {
            // given
            when(collection.countDocuments(any(ClientSession.class), any(Bson.class))).thenReturn(7L);

            // when
            Long size = join(queue().size(context));

            // then
            assertThat(size).isEqualTo(7L);
            ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
            verify(collection).countDocuments(eq(session), filter.capture());
            assertThat(render(filter.getValue()).toJson()).contains(PROCESSING_GROUP);
        }

        @Test
        void containsIsTrueWhenTheSequenceHasLetters() throws Exception {
            // given
            sequenceHasLetters(1);

            // when / then
            assertThat(join(queue().contains(SEQUENCE, context))).isTrue();
        }

        @Test
        void containsIsFalseWhenTheSequenceHasNoLetters() throws Exception {
            // given
            sequenceHasLetters(0);

            // when / then
            assertThat(join(queue().contains(SEQUENCE, context))).isFalse();
        }

        @Test
        void amountOfSequencesCountsTheDistinctSequenceIdentifiers() throws Exception {
            // given
            sequencesPresent("one", "two", "three");

            // when / then
            assertThat(join(queue().amountOfSequences(context))).isEqualTo(3L);
        }
    }

    @Nested
    class Clearing {

        @Test
        void deletesAllLettersOfTheProcessingGroupWithTheSessionOfTheProcessingContext() throws Exception {
            // given / when
            join(queue().clear(context));

            // then
            ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
            verify(collection).deleteMany(eq(session), filter.capture());
            assertThat(render(filter.getValue()).toJson()).contains(PROCESSING_GROUP);
        }
    }

    @Nested
    class Processing {

        @Test
        void completesWithFalseWhenNoLetterCanBeClaimed() throws Exception {
            // given
            aggregateWithoutResults();

            // when
            Boolean processed = join(queue().process(l -> CompletableFuture.failedFuture(new IllegalStateException()),
                                                     context));

            // then
            assertThat(processed).isFalse();
        }

        @Test
        void allowsDiskUseForTheAggregationByDefault() throws Exception {
            // given
            AggregateIterable<Document> aggregate = aggregateWithoutResults();

            // when
            join(queue().process(l -> CompletableFuture.failedFuture(new IllegalStateException()), context));

            // then
            verify(aggregate).allowDiskUse(true);
        }

        @Test
        void disallowsDiskUseForTheAggregationWhenConfiguredSo() throws Exception {
            // given
            AggregateIterable<Document> aggregate = aggregateWithoutResults();

            // when
            join(queue(MongoSequencedDeadLetterQueueConfiguration.DEFAULT.allowDiskUse(false))
                         .process(l -> CompletableFuture.failedFuture(new IllegalStateException()), context));

            // then
            verify(aggregate).allowDiskUse(false);
        }

        @SuppressWarnings("unchecked")
        private AggregateIterable<Document> aggregateWithoutResults() {
            AggregateIterable<Document> aggregate = mock(AggregateIterable.class);
            MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(cursor.hasNext()).thenReturn(false);
            when(aggregate.allowDiskUse(anyBoolean())).thenReturn(aggregate);
            when(aggregate.batchSize(anyInt())).thenReturn(aggregate);
            when(aggregate.iterator()).thenReturn(cursor);
            when(collection.aggregate(any(ClientSession.class), anyList())).thenReturn(aggregate);
            return aggregate;
        }
    }

    @Nested
    class Construction {

        @Test
        void constructsWithTheDefaultConfiguration() {
            assertThatCode(() -> construct("my_processing_group",
                                           collection,
                                           provider,
                                           eventConverter,
                                           jacksonConverter,
                                           MongoSequencedDeadLetterQueueConfiguration.DEFAULT))
                    .doesNotThrowAnyException();
        }

        @Test
        void createsTheIndexesByDefault() {
            // given
            ArgumentCaptor<IndexOptions> options = ArgumentCaptor.forClass(IndexOptions.class);

            // when
            construct("my_processing_group", collection, provider, eventConverter, jacksonConverter,
                      MongoSequencedDeadLetterQueueConfiguration.DEFAULT);

            // then
            verify(collection, times(3)).createIndex(any(ClientSession.class),
                                                               any(Bson.class),
                                                               options.capture());
            assertThat(options.getAllValues()).extracting(IndexOptions::getName)
                                              .containsExactly("processingGroupIdentifierIndex",
                                                               "processingGroupAndSequenceIdentifierIndex",
                                                               "uniqueDeadLetterEntryIndex");
        }

        @Test
        void skipsCreatingTheIndexesWhenDisabled() {
            // given / when
            construct("my_processing_group", collection, provider, eventConverter, jacksonConverter,
                      MongoSequencedDeadLetterQueueConfiguration.DEFAULT.ensureIndexes(false));

            // then
            verify(collection, never()).createIndex(any(ClientSession.class),
                                                              any(Bson.class),
                                                              any(IndexOptions.class));
        }

        @Test
        void nullProcessingGroupThrowsAxonConfigurationException() {
            assertThatThrownBy(() -> construct(null, collection, provider, eventConverter, jacksonConverter,
                                               MongoSequencedDeadLetterQueueConfiguration.DEFAULT))
                    .isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void emptyProcessingGroupThrowsAxonConfigurationException() {
            assertThatThrownBy(() -> construct("", collection, provider, eventConverter, jacksonConverter,
                                               MongoSequencedDeadLetterQueueConfiguration.DEFAULT))
                    .isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void nullDeadLetterCollectionThrowsAxonConfigurationException() {
            assertThatThrownBy(() -> construct("group", null, provider, eventConverter, jacksonConverter,
                                               MongoSequencedDeadLetterQueueConfiguration.DEFAULT))
                    .isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void nullTransactionalExecutorProviderThrowsAxonConfigurationException() {
            assertThatThrownBy(() -> construct("group", collection, null, eventConverter, jacksonConverter,
                                               MongoSequencedDeadLetterQueueConfiguration.DEFAULT))
                    .isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void nullEventConverterThrowsAxonConfigurationException() {
            assertThatThrownBy(() -> construct("group", collection, provider, null, jacksonConverter,
                                               MongoSequencedDeadLetterQueueConfiguration.DEFAULT))
                    .isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void nullGenericConverterThrowsAxonConfigurationException() {
            assertThatThrownBy(() -> construct("group", collection, provider, eventConverter, null,
                                               MongoSequencedDeadLetterQueueConfiguration.DEFAULT))
                    .isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void nullConfigurationThrowsAxonConfigurationException() {
            assertThatThrownBy(() -> construct("group", collection, provider, eventConverter, jacksonConverter,
                                               null))
                    .isInstanceOf(AxonConfigurationException.class);
        }
    }

    @Nested
    class TheConfiguration {

        private final MongoSequencedDeadLetterQueueConfiguration defaults =
                MongoSequencedDeadLetterQueueConfiguration.DEFAULT;

        @Test
        void defaultsMatchTheDocumentedValues() {
            assertThat(defaults.converter()).isInstanceOf(EventMessageDeadLetterMongoConverter.class);
            assertThat(defaults.maxSequences()).isEqualTo(1024);
            assertThat(defaults.maxSequenceSize()).isEqualTo(1024);
            assertThat(defaults.queryPageSize()).isEqualTo(100);
            assertThat(defaults.claimDuration()).isEqualTo(Duration.ofSeconds(30));
            assertThat(defaults.ensureIndexes()).isTrue();
            assertThat(defaults.allowDiskUse()).isTrue();
        }

        @Test
        void withMethodsReturnAnAdjustedCopyAndLeaveTheOriginalUntouched() {
            // given
            DeadLetterMongoConverter<EventMessage> customConverter = new EventMessageDeadLetterMongoConverter();

            // when
            MongoSequencedDeadLetterQueueConfiguration result = defaults.converter(customConverter)
                                                                        .maxSequences(1)
                                                                        .maxSequenceSize(2)
                                                                        .queryPageSize(3)
                                                                        .claimDuration(Duration.ofMinutes(4))
                                                                        .ensureIndexes(false)
                                                                        .allowDiskUse(false);

            // then
            assertThat(result.converter()).isSameAs(customConverter);
            assertThat(result.maxSequences()).isEqualTo(1);
            assertThat(result.maxSequenceSize()).isEqualTo(2);
            assertThat(result.queryPageSize()).isEqualTo(3);
            assertThat(result.claimDuration()).isEqualTo(Duration.ofMinutes(4));
            assertThat(result.ensureIndexes()).isFalse();
            assertThat(result.allowDiskUse()).isFalse();
            assertThat(defaults.maxSequences()).isEqualTo(1024);
            assertThat(defaults.ensureIndexes()).isTrue();
        }

        @Test
        void nullConverterThrowsAxonConfigurationException() {
            assertThatThrownBy(() -> defaults.converter(null)).isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void nonPositiveMaxSequencesThrowsAxonConfigurationException() {
            assertThatThrownBy(() -> defaults.maxSequences(0)).isInstanceOf(AxonConfigurationException.class);
            assertThatThrownBy(() -> defaults.maxSequences(-1)).isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void nonPositiveMaxSequenceSizeThrowsAxonConfigurationException() {
            assertThatThrownBy(() -> defaults.maxSequenceSize(0)).isInstanceOf(AxonConfigurationException.class);
            assertThatThrownBy(() -> defaults.maxSequenceSize(-1)).isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void nonPositiveQueryPageSizeThrowsAxonConfigurationException() {
            assertThatThrownBy(() -> defaults.queryPageSize(0)).isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void nullClaimDurationThrowsAxonConfigurationException() {
            assertThatThrownBy(() -> defaults.claimDuration(null)).isInstanceOf(AxonConfigurationException.class);
        }
    }

    @Nested
    class AddonRegistration {

        @Test
        void constructingTheQueueRegistersTheMongoAddonWithTheEntitlementManager() {
            // given / when
            construct("my_processing_group", collection, provider, eventConverter, jacksonConverter,
                      MongoSequencedDeadLetterQueueConfiguration.DEFAULT);

            // then
            // claimMessage must not fail with an "addon not found" error, which would indicate the addon was never
            // registered. Other exceptions (e.g. grace period expired) are unrelated to registration.
            try {
                EntitlementManager.INSTANCE.claimMessage(MongoAxoniqAddon.IDENTIFIER,
                                                         EntitlementMessageType.EVENT,
                                                         1);
            } catch (AxoniqAddonNotGrantedException e) {
                assertThat(e.getMessage())
                        .as("MongoDB addon should be registered with the entitlement system")
                        .doesNotContain("Addon was not found in the application");
            }
        }
    }
}
