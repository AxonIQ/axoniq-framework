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

package io.axoniq.framework.messaging.eventhandling.deadletter.jpa;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.Persistence;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.IdentifierFactory;
import org.axonframework.common.jpa.EntityManagerExecutor;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.conversion.CachingSupplier;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.GenericDeadLetter;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueueTest;
import io.axoniq.framework.messaging.deadletter.WrongDeadLetterTypeException;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GapAwareTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tools.jackson.databind.DefaultTyping;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;

import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.*;

class JpaSequencedDeadLetterQueueTest extends SequencedDeadLetterQueueTest<EventMessage> {

    private static final int MAX_SEQUENCES_AND_SEQUENCE_SIZE = 64;
    private final AtomicLong sequenceCounter = new AtomicLong(0);

    private final EntityManagerFactory emf = Persistence.createEntityManagerFactory("dlq");
    private final EntityManager entityManager = emf.createEntityManager();
    private final JacksonConverter jacksonConverter = new JacksonConverter();
    private final DelegatingEventConverter eventConverter = new DelegatingEventConverter(jacksonConverter);
    private EntityTransaction transaction;

    @BeforeEach
    public void setUpJpa() {
        transaction = entityManager.getTransaction();
        transaction.begin();
    }

    @AfterEach
    public void rollback() {
        transaction.rollback();
    }

    @Override
    protected void setClock(Clock clock) {
        GenericDeadLetter.clock = clock;
    }

    @Override
    protected long maxSequences() {
        return MAX_SEQUENCES_AND_SEQUENCE_SIZE;
    }

    @Override
    protected long maxSequenceSize() {
        return MAX_SEQUENCES_AND_SEQUENCE_SIZE;
    }

    @Override
    public DeadLetter<EventMessage> generateInitialLetter() {
        return new GenericDeadLetter<>("sequenceIdentifier", generateEvent(), generateThrowable(),
                                      buildTestContext());
    }

    @Override
    protected DeadLetter<EventMessage> generateFollowUpLetter() {
        return new GenericDeadLetter<>("sequenceIdentifier", generateEvent(), (Throwable) null,
                                      buildTestContext());
    }

    private Context buildTestContext() {
        long seqNo = sequenceCounter.getAndIncrement();
        return Context.with(TrackingToken.RESOURCE_KEY, new GapAwareTrackingToken(seqNo, Collections.emptyList()))
                      .withResource(LegacyResources.AGGREGATE_TYPE_KEY, "TestAggregate")
                      .withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "aggregate-" + seqNo)
                      .withResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY, seqNo);
    }

    @Override
    protected DeadLetter<EventMessage> mapToQueueImplementation(DeadLetter<EventMessage> deadLetter) {
        if (deadLetter instanceof JpaDeadLetter) {
            return deadLetter;
        }
        if (deadLetter instanceof GenericDeadLetter) {
            return new JpaDeadLetter<>(
                    IdentifierFactory.getInstance().generateIdentifier(),
                    0L,
                    ((GenericDeadLetter<EventMessage>) deadLetter).getSequenceIdentifier().toString(),
                    deadLetter.enqueuedAt(),
                    deadLetter.lastTouched(),
                    deadLetter.cause().orElse(null),
                    deadLetter.diagnostics(),
                    deadLetter.message(),
                    deadLetter.context()
            );
        }
        throw new IllegalArgumentException("Can not map dead letter of type " + deadLetter.getClass().getName());
    }

    @Override
    protected DeadLetter<EventMessage> generateRequeuedLetter(DeadLetter<EventMessage> original,
                                                              Instant lastTouched,
                                                              Throwable requeueCause,
                                                              Metadata diagnostics) {
        setAndGetTime(lastTouched);
        return original.withCause(requeueCause).withDiagnostics(diagnostics).markTouched();
    }

    @Override
    protected void assertMessage(EventMessage expected, EventMessage actual) {
        assertThat(actual.identifier()).isEqualTo(expected.identifier());
        assertThat(actual.type()).isEqualTo(expected.type());
        assertThat(actual.metadata()).isEqualTo(expected.metadata());

        // Payload is stored as raw bytes; deserialize to compare with the original
        Object deserializedPayload = eventConverter.convertPayload(actual, expected.payloadType());
        assertThat(deserializedPayload).isEqualTo(expected.payload());
    }

    @Override
    protected ProcessingContext toProcessingContext(Context context) {
        ProcessingContext pc = super.toProcessingContext(context);
        if (pc != null) {
            pc.putResource(JpaTransactionalExecutorProvider.SUPPLIER_KEY,
                           CachingSupplier.of(() -> new EntityManagerExecutor(() -> entityManager)));
        }
        return pc;
    }

    @Override
    public SequencedDeadLetterQueue<EventMessage> buildTestSubject() {
        return JpaSequencedDeadLetterQueue
                .builder()
                .transactionalExecutorProvider(testTransactionalExecutorProvider())
                .maxSequences(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                .maxSequenceSize(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                .processingGroup("my_processing_group")
                .eventConverter(eventConverter)
                .genericConverter(jacksonConverter)
                .build();
    }

    /**
     * Creates a {@link TransactionalExecutorProvider} that delegates to the real
     * {@link JpaTransactionalExecutorProvider} when a {@link ProcessingContext} is available (exercising the production
     * code path that extracts the executor from the context), and falls back to the test's single {@link EntityManager}
     * when the context is {@code null}.
     * <p>
     * The fallback is necessary because this test uses rollback-based isolation: a single {@link EntityManager} with a
     * transaction opened in {@code @BeforeEach} and rolled back in {@code @AfterEach}. This transaction also serves as
     * the transaction management that {@link EntityManagerExecutor} assumes is already active (it does not begin/commit
     * on its own — in production, the real {@link ProcessingContext} lifecycle handles that).
     * <p>
     * When the context is {@code null}, {@link JpaTransactionalExecutorProvider} creates a
     * <em>second</em> {@link EntityManager} with its own transaction. That second transaction
     * blocks on table locks held by the test's transaction, while the test thread blocks waiting for the operation to
     * complete — a classic deadlock. The fallback avoids this by routing all null-context operations through the test's
     * existing {@link EntityManager} and transaction.
     */
    private JpaTransactionalExecutorProvider testTransactionalExecutorProvider() {
        return new JpaTransactionalExecutorProvider(emf) {
            @Override
            @NonNull
            public TransactionalExecutor<EntityManager> getTransactionalExecutor(
                    @Nullable ProcessingContext processingContext
            ) {
                if (processingContext != null) {
                    return super.getTransactionalExecutor(processingContext);
                }
                return new EntityManagerExecutor(() -> entityManager);
            }
        };
    }

    @Test
    void buildWithNegativeMaxQueuesThrowsAxonConfigurationException() {
        JpaSequencedDeadLetterQueue.Builder<EventMessage> builderTestSubject = JpaSequencedDeadLetterQueue.builder();

        assertThatThrownBy(() -> builderTestSubject.maxSequences(-1)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithZeroMaxQueuesThrowsAxonConfigurationException() {
        JpaSequencedDeadLetterQueue.Builder<EventMessage> builderTestSubject = JpaSequencedDeadLetterQueue.builder();

        assertThatThrownBy(() -> builderTestSubject.maxSequences(0)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithNegativeMaxQueueSizeThrowsAxonConfigurationException() {
        JpaSequencedDeadLetterQueue.Builder<EventMessage> builderTestSubject = JpaSequencedDeadLetterQueue.builder();

        assertThatThrownBy(() -> builderTestSubject.maxSequenceSize(-1)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithZeroMaxQueueSizeThrowsAxonConfigurationException() {
        JpaSequencedDeadLetterQueue.Builder<EventMessage> builderTestSubject = JpaSequencedDeadLetterQueue.builder();

        assertThatThrownBy(() -> builderTestSubject.maxSequenceSize(0)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void canNotSetNegativeQueryPageSize() {
        JpaSequencedDeadLetterQueue.Builder<EventMessage> builder = JpaSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> builder.queryPageSize(-1)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void canNotSetZeroQueryPageSize() {
        JpaSequencedDeadLetterQueue.Builder<EventMessage> builder = JpaSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> builder.queryPageSize(0)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void enqueuedLetterWithoutAggregateResourcesPreservesTrackingToken() {
        // given
        SequencedDeadLetterQueue<EventMessage> queue = buildTestSubject();
        Object sequenceId = generateId();
        GlobalSequenceTrackingToken expectedToken = new GlobalSequenceTrackingToken(42L);
        Context contextWithTokenOnly = Context.with(TrackingToken.RESOURCE_KEY, expectedToken);
        DeadLetter<EventMessage> letter = new GenericDeadLetter<>(
                "sequenceIdentifier", generateEvent(), generateThrowable(), contextWithTokenOnly
        );

        // when
        queue.enqueue(sequenceId, letter, toProcessingContext(contextWithTokenOnly)).join();

        // then
        Iterator<DeadLetter<? extends EventMessage>> result =
                queue.deadLetterSequence(sequenceId, null).join().iterator();
        assertThat(result.hasNext()).isTrue();
        JpaDeadLetter<? extends EventMessage> retrieved = (JpaDeadLetter<? extends EventMessage>) result.next();

        assertThat(retrieved.context().getResource(TrackingToken.RESOURCE_KEY))
                .isEqualTo(expectedToken);
        assertThat(retrieved.context().containsResource(LegacyResources.AGGREGATE_TYPE_KEY))
                .isFalse();
        assertThat(retrieved.context().containsResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY))
                .isFalse();
        assertThat(retrieved.context().containsResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY))
                .isFalse();
    }

    @Test
    void cannotRequeueGenericDeadLetter() {
        SequencedDeadLetterQueue<EventMessage> queue = buildTestSubject();
        DeadLetter<EventMessage> letter = generateInitialLetter();
        assertThatThrownBy(() -> queue.requeue(letter, d -> d, null).join())
                .isInstanceOf(CompletionException.class)
                .cause().isInstanceOf(WrongDeadLetterTypeException.class);
    }

    @Test
    void cannotEvictGenericDeadLetter() {
        SequencedDeadLetterQueue<EventMessage> queue = buildTestSubject();
        DeadLetter<EventMessage> letter = generateInitialLetter();
        assertThatThrownBy(() -> queue.evict(letter, null).join())
                .isInstanceOf(CompletionException.class)
                .cause().isInstanceOf(WrongDeadLetterTypeException.class);
    }

    @Test
    void canNotSetProcessingGroupToEmpty() {
        JpaSequencedDeadLetterQueue.Builder<EventMessage> builder = JpaSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> builder.processingGroup("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void canNotSetProcessingGroupToNull() {
        JpaSequencedDeadLetterQueue.Builder<EventMessage> builder = JpaSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> builder.processingGroup(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void canNotBuildWithoutProcessingGroup() {
        JacksonConverter jacksonConverter = new JacksonConverter();
        JpaSequencedDeadLetterQueue.Builder<EventMessage> builder = JpaSequencedDeadLetterQueue
                .builder()
                .transactionalExecutorProvider(new JpaTransactionalExecutorProvider(emf))
                .eventConverter(new DelegatingEventConverter(jacksonConverter))
                .genericConverter(jacksonConverter);
        assertThatThrownBy(builder::build).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void canNotBuildWithoutTransactionalExecutorProvider() {
        JacksonConverter jacksonConverter = new JacksonConverter();
        JpaSequencedDeadLetterQueue.Builder<EventMessage> builder = JpaSequencedDeadLetterQueue
                .builder()
                .processingGroup("my_processing_group")
                .eventConverter(new DelegatingEventConverter(jacksonConverter))
                .genericConverter(jacksonConverter);

        assertThatThrownBy(builder::build).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void canNotSetNullConverter() {
        assertThatThrownBy(() -> JpaSequencedDeadLetterQueue.builder().converter(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Nested
    class WithDefaultTypingConverter {

        // Mirrors the setup needed to stay wire-compatible with an Axon Framework 4 event store written by
        // JacksonSerializer/Jackson3Serializer with default typing.
        private static JacksonConverter defaultTypingConverter(DefaultTyping defaultTyping) {
            BasicPolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                                                                             .allowIfSubType("java.util.")
                                                                             .allowIfSubType("org.axonframework.")
                                                                             .build();
            return new JacksonConverter(
                    JsonMapper.builder()
                              .polymorphicTypeValidator(ptv)
                              .activateDefaultTyping(ptv, defaultTyping)
                              .build()
            );
        }

        @ParameterizedTest
        @EnumSource(value = DefaultTyping.class,
                    names = {"OBJECT_AND_NON_CONCRETE", "NON_CONCRETE_AND_ARRAYS", "NON_FINAL"})
        void readsBackDiagnosticsThroughDeadLettersWhenConverterUsesDefaultTyping(DefaultTyping defaultTyping) {
            // given
            JacksonConverter defaultTypingJacksonConverter = defaultTypingConverter(defaultTyping);
            DelegatingEventConverter defaultTypingEventConverter =
                    new DelegatingEventConverter(defaultTypingJacksonConverter);
            SequencedDeadLetterQueue<EventMessage> queue = JpaSequencedDeadLetterQueue
                    .<EventMessage>builder()
                    .transactionalExecutorProvider(testTransactionalExecutorProvider())
                    .maxSequences(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                    .maxSequenceSize(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                    .processingGroup("default_typing_processing_group")
                    .eventConverter(defaultTypingEventConverter)
                    .genericConverter(defaultTypingJacksonConverter)
                    .build();
            Object sequenceId = generateId();
            Metadata diagnostics = Metadata.with("retries", "3");
            Context context = buildTestContext();
            DeadLetter<EventMessage> letter = new GenericDeadLetter<>(
                    "sequenceIdentifier", generateEvent(), generateThrowable(), context
            ).withDiagnostics(diagnostics);

            // when
            queue.enqueue(sequenceId, letter, toProcessingContext(context)).join();
            Iterator<Iterable<DeadLetter<? extends EventMessage>>> sequences =
                    queue.deadLetters(null).join().iterator();

            // then
            assertThat(sequences.hasNext()).isTrue();
            Iterator<DeadLetter<? extends EventMessage>> sequence = sequences.next().iterator();
            assertThat(sequence.hasNext()).isTrue();
            DeadLetter<? extends EventMessage> retrieved = sequence.next();
            assertThat(retrieved.diagnostics()).isEqualTo(diagnostics);
        }
    }
}
