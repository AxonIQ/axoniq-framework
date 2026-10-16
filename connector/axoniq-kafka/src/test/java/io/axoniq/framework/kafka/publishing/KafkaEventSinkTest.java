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

package io.axoniq.framework.kafka.publishing;

import io.axoniq.framework.kafka.codec.CodecTestFixtures;
import io.axoniq.framework.kafka.codec.DefaultKafkaEventCodec;
import io.axoniq.framework.kafka.utils.RecordingEntitlementManager;
import io.axoniq.framework.kafka.utils.RecordingKafkaClientFactory;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.axonframework.common.FutureUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.AnnotationMessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static io.axoniq.framework.kafka.codec.CodecTestFixtures.header;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class KafkaEventSinkTest {

    private static final String TOPIC = "parcels";

    private final DefaultKafkaEventCodec codec =
            CodecTestFixtures.defaultCodec();
    private final KafkaEventSinkConfiguration baseConfiguration =
            KafkaEventSinkConfiguration.create(codec, TopicResolver.fixed(TOPIC));

    record ParcelDispatched(String parcelId) {

    }

    private static EventMessage parcel(String parcelId) {
        return EventTestUtils.asEventMessage(new ParcelDispatched(parcelId));
    }

    private static void assertPublicationFailure(CompletableFuture<?> result, String rootCauseMessage) {
        assertThat(result).failsWithin(Duration.ofSeconds(5))
                          .withThrowableThat()
                          .satisfies(e -> assertThat(Stream.iterate(e, Objects::nonNull, Throwable::getCause))
                                  .anyMatch(KafkaPublicationException.class::isInstance))
                          .havingRootCause()
                          .withMessage(rootCauseMessage);
    }

    @Nested
    class Resolution {

        private final RecordingKafkaClientFactory clientFactory = new RecordingKafkaClientFactory(true);

        @Test
        void publishesToResolvedTopicWithResolvedKey() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(
                    clientFactory,
                    baseConfiguration.withConfirmationMode(ConfirmationMode.NONE)
                                     .withRecordKeyResolver((e, c) -> e.payloadAs(ParcelDispatched.class).parcelId())
            );

            // when
            testSubject.publish(null, parcel("p-1")).orTimeout(30, TimeUnit.SECONDS).join();

            // then
            ProducerRecord<String, byte[]> sent = clientFactory.<byte[]>producers().getFirst().history().getFirst();
            assertThat(sent.topic()).isEqualTo(TOPIC);
            assertThat(sent.key()).isEqualTo("p-1");
        }

        @Test
        void skipsEventsWithoutTopic() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(
                    clientFactory,
                    KafkaEventSinkConfiguration.create(codec, (e, c) -> e.payloadAs(ParcelDispatched.class)
                                                                          .parcelId().equals("p-1")
                                                      ? Optional.of(TOPIC) : Optional.empty())
            );

            // when
            testSubject.publish(null, parcel("p-1"), parcel("p-2")).orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(clientFactory.<byte[]>producers().getFirst().history())
                    .singleElement().extracting(ProducerRecord::topic).isEqualTo(TOPIC);
        }

        @Test
        void createsNoProducerWhenNothingIsPublished() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(
                    clientFactory, KafkaEventSinkConfiguration.create(codec, (e, c) -> Optional.empty())
            );

            // when
            CompletableFuture<Void> result = testSubject.publish(null, parcel("p-1"));

            // then
            assertThat(result).isCompleted();
            assertThat(clientFactory.producers()).isEmpty();
        }

        @Test
        void passesSinkSpecificProducerProperties() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(
                    clientFactory, baseConfiguration.withProducerProperties(Map.of(ProducerConfig.LINGER_MS_CONFIG, 5))
            );

            // when
            testSubject.publish(null, parcel("p-1")).orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(clientFactory.producerProperties().getFirst()).containsEntry(ProducerConfig.LINGER_MS_CONFIG, 5);
        }
    }

    @Nested
    class PublishingPayloads {

        @Event(namespace = "parcels", name = "ParcelShipped", version = "2.1")
        record ParcelShipped(String parcelId) {

        }

        private final RecordingKafkaClientFactory clientFactory = new RecordingKafkaClientFactory(true);

        @Test
        void resolvesPayloadTypeHonoringEventAnnotation() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(clientFactory, baseConfiguration);

            // when
            EventSinks.publishPayloads(testSubject, null, new AnnotationMessageTypeResolver(),
                                       new ParcelShipped("p-1"))
                      .orTimeout(5, TimeUnit.SECONDS).join();

            // then
            ProducerRecord<String, byte[]> sent = clientFactory.<byte[]>producers().getFirst().history().getFirst();
            assertThat(header(sent.headers(), "axon-type")).isEqualTo("parcels.ParcelShipped");
            assertThat(header(sent.headers(), "axon-version")).isEqualTo("2.1");
        }

        @Test
        void resolvesPayloadTypeWithConfiguredResolver() {
            // given
            MessageType custom = new MessageType("com.example.Custom", "7");
            KafkaEventSink testSubject = new KafkaEventSink(clientFactory, baseConfiguration);

            // when
            EventSinks.publishPayloads(testSubject, null, payloadType -> Optional.of(custom),
                                       new ParcelShipped("p-1"))
                      .orTimeout(5, TimeUnit.SECONDS).join();

            // then
            ProducerRecord<String, byte[]> sent = clientFactory.<byte[]>producers().getFirst().history().getFirst();
            assertThat(header(sent.headers(), "axon-type")).isEqualTo("com.example.Custom");
        }

        @Test
        void unresolvablePayloadTypeFailsWithoutPublishingAnything() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(clientFactory, baseConfiguration);

            // when
            CompletableFuture<Void> result = EventSinks.publishPayloads(testSubject, null,
                                                                        payloadType -> Optional.empty(),
                                                                        new ParcelShipped("p-1"));

            // then
            assertThat(result).failsWithin(Duration.ofSeconds(5));
            assertThat(clientFactory.producers()).isEmpty();
        }

        @Test
        void nullPayloadFailsWithoutPublishingAnything() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(clientFactory, baseConfiguration);
            List<Object> events = Arrays.asList(new ParcelShipped("p-1"), null);

            // when
            CompletableFuture<Void> result = EventSinks.publishPayloads(testSubject, null,
                                                                        new AnnotationMessageTypeResolver(), events);

            // then
            assertThat(result).failsWithin(Duration.ofSeconds(5))
                              .withThrowableThat()
                              .withRootCauseInstanceOf(NullPointerException.class);
            assertThat(clientFactory.producers()).isEmpty();
        }

        @Test
        void publishesEventMessagesAsTheyAre() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(clientFactory, baseConfiguration);
            EventMessage message = parcel("p-1");

            // when
            EventSinks.publishPayloads(testSubject, null, new AnnotationMessageTypeResolver(), List.of(message))
                      .orTimeout(5, TimeUnit.SECONDS).join();

            // then
            ProducerRecord<String, byte[]> sent = clientFactory.<byte[]>producers().getFirst().history().getFirst();
            assertThat(header(sent.headers(), "axon-id")).isEqualTo(message.identifier());
            assertThat(header(sent.headers(), "axon-type")).isEqualTo(message.type().name());
        }
    }

    @Nested
    class RecordKeys {

        private final EventMessage withParcelId = parcel("p-1").andMetadata(Map.of("parcelId", "p-1"));

        @Test
        void fromMetadataUsesTheMetadataEntry() {
            // when
            String key = RecordKeyResolver.fromMetadata("parcelId").resolve(withParcelId, null);

            // then
            assertThat(key).isEqualTo("p-1");
        }

        @Test
        void fromMetadataPublishesWithoutKeyWhenTheEntryIsMissing() {
            // when
            String key = RecordKeyResolver.fromMetadata("orderId").resolve(withParcelId, null);

            // then
            assertThat(key).isNull();
        }

        @Test
        void noneNeverProducesAKey() {
            // when
            String key = RecordKeyResolver.none().resolve(withParcelId, null);

            // then
            assertThat(key).isNull();
        }
    }

    @Nested
    class WithoutConfirmation {

        private final RecordingKafkaClientFactory clientFactory = new RecordingKafkaClientFactory(false);
        private final KafkaEventSink testSubject =
                new KafkaEventSink(clientFactory, baseConfiguration.withConfirmationMode(ConfirmationMode.NONE));

        @Test
        void completesWithoutWaitingForKafka() {
            // when
            CompletableFuture<Void> result = testSubject.publish(null, parcel("p-1"));

            // then
            assertThat(result).isCompleted();
        }

        @Test
        void deliveryFailureDoesNotFailTheProcessingContext() {
            // when
            CompletableFuture<Void> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-1"))
            );
            clientFactory.producers().getFirst().errorNext(new RuntimeException("broker down"));

            // then
            assertThat(result).succeedsWithin(Duration.ofSeconds(5));
        }
    }

    @Nested
    class WaitingForAcknowledgement {

        private final RecordingKafkaClientFactory clientFactory = new RecordingKafkaClientFactory(false);

        @Test
        void outsideContextCompletesOnAcknowledgement() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(clientFactory, baseConfiguration);

            // when
            CompletableFuture<Void> result = testSubject.publish(null, parcel("p-1"));

            // then
            assertThat(result).isNotDone();
            clientFactory.producers().getFirst().completeNext();
            assertThat(result).isCompleted();
        }

        @Test
        void outsideContextFailsOnDeliveryFailure() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(clientFactory, baseConfiguration);

            // when
            CompletableFuture<Void> result = testSubject.publish(null, parcel("p-1"));
            clientFactory.producers().getFirst().errorNext(new RuntimeException("broker down"));

            // then
            assertPublicationFailure(result, "broker down");
        }

        @Test
        void contextCommitsOnlyAfterAllRecordsAreAcknowledged() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(clientFactory, baseConfiguration);

            // when
            CompletableFuture<Void> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-1"))
                                      .thenCompose(r -> testSubject.publish(ctx, parcel("p-2")))
            );

            // then
            MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
            await().atMost(Duration.ofSeconds(5)).until(() -> producer.history().size() == 2);
            assertThat(result).isNotDone();
            producer.completeNext();
            assertThat(result).isNotDone();
            producer.completeNext();
            assertThat(result).succeedsWithin(Duration.ofSeconds(5));
        }

        @Test
        void deliveryFailureFailsTheContext() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(clientFactory, baseConfiguration);

            // when
            CompletableFuture<Void> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-1"))
            );
            await().atMost(Duration.ofSeconds(5)).until(() -> !clientFactory.producers().isEmpty()
                    && clientFactory.producers().getFirst().history().size() == 1);
            clientFactory.producers().getFirst().errorNext(new RuntimeException("broker down"));

            // then
            assertPublicationFailure(result, "broker down");
        }

        @Test
        void missingAcknowledgementFailsTheContextAfterTimeout() {
            // given
            KafkaEventSink testSubject =
                    new KafkaEventSink(clientFactory, baseConfiguration.withAckTimeout(Duration.ofMillis(100)));

            // when
            CompletableFuture<Void> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-1"))
            );

            // then
            assertThat(result).failsWithin(Duration.ofSeconds(5))
                              .withThrowableThat().withRootCauseInstanceOf(TimeoutException.class);
        }
    }

    @Nested
    class Transactional {

        private final RecordingKafkaClientFactory clientFactory = new RecordingKafkaClientFactory(true);
        private final KafkaEventSink testSubject = new KafkaEventSink(
                clientFactory,
                baseConfiguration.withConfirmationMode(ConfirmationMode.TRANSACTIONAL)
                                 .withTransactionalIdPrefix("parcels-1-")
        );

        @Test
        void requiresTransactionalIdPrefix() {
            // given
            KafkaEventSinkConfiguration configuration =
                    baseConfiguration.withConfirmationMode(ConfirmationMode.TRANSACTIONAL);

            // when / then
            assertThatThrownBy(() -> new KafkaEventSink(clientFactory, configuration))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("transactional identifier prefix");
        }

        @Test
        void publishesAllEventsOfContextInOneCommittedTransaction() {
            // when
            UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-1"))
                                      .thenCompose(r -> testSubject.publish(ctx, parcel("p-2")))
            ).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(clientFactory.producers()).hasSize(1);
            MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
            assertThat(producer.transactionCommitted()).isTrue();
            assertThat(producer.history()).hasSize(2);
            assertThat(clientFactory.producerProperties().getFirst())
                    .containsEntry(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "parcels-1-0");
        }

        @Test
        void abortsTransactionWhenContextFailsAndReusesProducer() {
            // when
            CompletableFuture<Object> failed = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-1"))
                                      .thenCompose(r -> CompletableFuture.failedFuture(
                                              new IllegalStateException("boom")
                                      ))
            );

            // then
            assertThat(failed).failsWithin(Duration.ofSeconds(5));
            MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
            assertThat(producer.transactionAborted()).isTrue();
            assertThat(producer.history()).isEmpty();

            // when - the next context runs on the same producer, returned to the pool exactly once
            UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(ctx -> testSubject.publish(ctx, parcel("p-2")))
                               .orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(clientFactory.producers()).hasSize(1);
            assertThat(producer.history()).hasSize(1);
        }

        @Test
        void failedKafkaCommitFailsTheContextAndDiscardsProducer() {
            // given
            testSubject.publish(null, parcel("warm-up")).orTimeout(30, TimeUnit.SECONDS).join();
            MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
            producer.commitTransactionException = new RuntimeException("commit failed");
            producer.abortTransactionException = new RuntimeException("abort failed");

            // when
            CompletableFuture<Void> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-1"))
            );

            // then
            assertPublicationFailure(result, "commit failed");
            assertThat(producer.closed()).isTrue();

            // when
            testSubject.publish(null, parcel("p-2")).orTimeout(30, TimeUnit.SECONDS).join();

            // then
            // the replacement reuses the identifier of the discarded producer, fencing it off should it linger
            assertThat(clientFactory.producers()).hasSize(2);
            assertThat(clientFactory.producerProperties().get(1))
                    .containsEntry(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "parcels-1-0");
        }

        @Test
        void sendFailureWithinContextPreventsCommit() {
            // given
            testSubject.publish(null, parcel("warm-up")).orTimeout(30, TimeUnit.SECONDS).join();
            MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
            producer.sendException = new RuntimeException("send failed");

            // when - the handler ignores the failed publication
            CompletableFuture<Void> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-1")).exceptionally(e -> null)
            );

            // then
            assertThat(result).failsWithin(Duration.ofSeconds(5));
            assertThat(producer.transactionAborted()).isTrue();
            assertThat(producer.history()).hasSize(1); // the warm-up only
        }

        @Test
        void concurrentContextsUseSeparateProducers() {
            // given
            CompletableFuture<Void> secondPublished = new CompletableFuture<>();
            CompletableFuture<Void> release = new CompletableFuture<>();

            // when
            CompletableFuture<Void> first = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-1")).thenCompose(r -> release)
            );
            CompletableFuture<Void> second = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-2"))
                                      .whenComplete((r, e) -> secondPublished.complete(null))
            );
            secondPublished.orTimeout(5, TimeUnit.SECONDS).join();
            release.complete(null);

            // then
            assertThat(List.of(first, second)).allSatisfy(f -> assertThat(f).succeedsWithin(Duration.ofSeconds(5)));
            assertThat(clientFactory.producers()).hasSize(2);
        }

        @Test
        void outsideContextCommitsImmediately() {
            // when
            testSubject.publish(null, parcel("p-1")).orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(clientFactory.<byte[]>producers().getFirst().transactionCommitted()).isTrue();
        }

        @Test
        void commitsOnTheCommittingThreadWithoutACommitExecutor() {
            // when
            CompletableFuture<Void> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-1"))
            );

            // then - nothing was handed off, so the commit completed before the context's future was returned
            assertThat(result).isCompleted();
            assertThat(clientFactory.<byte[]>producers().getFirst().transactionCommitted()).isTrue();
        }

        @Nested
        class WithACommitExecutor {

            private final List<Runnable> commits = new CopyOnWriteArrayList<>();
            private final KafkaEventSink testSubject = new KafkaEventSink(
                    clientFactory,
                    baseConfiguration.withConfirmationMode(ConfirmationMode.TRANSACTIONAL)
                                     .withTransactionalIdPrefix("parcels-1-")
                                     .withCommitExecutor(commits::add)
            );

            @Test
            void commitsOnTheExecutorAndCompletesTheContextAfterwards() {
                // when
                CompletableFuture<Void> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                        ctx -> testSubject.publish(ctx, parcel("p-1"))
                );

                // then - the commit waits for the executor
                MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
                assertThat(commits).hasSize(1);
                assertThat(producer.transactionCommitted()).isFalse();
                assertThat(result).isNotDone();

                // when
                commits.getFirst().run();

                // then
                assertThat(result).succeedsWithin(Duration.ofSeconds(5));
                assertThat(producer.transactionCommitted()).isTrue();
            }

            @Test
            void returnsTheProducerToThePoolExactlyOnce() {
                // given
                CompletableFuture<Void> first = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                        ctx -> testSubject.publish(ctx, parcel("p-1"))
                );
                commits.getFirst().run();
                first.orTimeout(5, TimeUnit.SECONDS).join();

                // when
                CompletableFuture<Void> second = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                        ctx -> testSubject.publish(ctx, parcel("p-2"))
                );
                commits.get(1).run();

                // then - the second context ran on the producer of the first
                assertThat(second).succeedsWithin(Duration.ofSeconds(5));
                assertThat(clientFactory.producers()).hasSize(1);
                assertThat(clientFactory.<byte[]>producers().getFirst().commitCount()).isEqualTo(2);
            }

            @Test
            void rejectedCommitAbortsTheTransactionAndReturnsTheProducer() {
                // given
                KafkaEventSink rejecting = new KafkaEventSink(
                        clientFactory,
                        baseConfiguration.withConfirmationMode(ConfirmationMode.TRANSACTIONAL)
                                         .withTransactionalIdPrefix("parcels-1-")
                                         .withCommitExecutor(task -> {
                                             throw new RejectedExecutionException("executor shut down");
                                         })
                );

                // when
                CompletableFuture<Void> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                        ctx -> rejecting.publish(ctx, parcel("p-1"))
                );

                // then
                assertPublicationFailure(result, "executor shut down");
                MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
                assertThat(producer.transactionAborted()).isTrue();
                assertThat(producer.closed()).isFalse();
            }
        }
    }

    @Nested
    class PublishingLateOrFailing {

        private final RecordingKafkaClientFactory clientFactory = new RecordingKafkaClientFactory(true);
        private final KafkaEventSink transactional = new KafkaEventSink(
                clientFactory,
                baseConfiguration.withConfirmationMode(ConfirmationMode.TRANSACTIONAL)
                                 .withTransactionalIdPrefix("parcels-1-")
        );
        private final KafkaEventSink awaitingAcks = new KafkaEventSink(
                clientFactory, baseConfiguration.withConfirmationMode(ConfirmationMode.WAIT_FOR_ACK)
        );

        @Test
        void publishWhileTheContextCommitsIsRejectedWithoutBorrowingAProducer() {
            // given
            AtomicReference<@Nullable CompletableFuture<Void>> late = new AtomicReference<>();

            // when
            UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(ctx -> {
                ctx.runOnAfterCommit(c -> late.set(transactional.publish(c, parcel("late"))));
                return FutureUtils.emptyCompletedFuture();
            }).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(late.get()).failsWithin(Duration.ofSeconds(5))
                                  .withThrowableThat()
                                  .havingRootCause()
                                  .isInstanceOf(IllegalStateException.class);
            assertThat(clientFactory.producers()).isEmpty();
        }

        @Test
        void publishAfterTheKafkaTransactionCommittedIsRejected() {
            // given
            AtomicReference<@Nullable CompletableFuture<Void>> late = new AtomicReference<>();

            // when
            UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(ctx -> {
                ctx.runOnAfterCommit(c -> late.set(transactional.publish(c, parcel("late"))));
                return transactional.publish(ctx, parcel("p-1"));
            }).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(late.get()).failsWithin(Duration.ofSeconds(5))
                                  .withThrowableThat()
                                  .havingRootCause()
                                  .isInstanceOf(IllegalStateException.class)
                                  .withMessageContaining("already been committed");
            MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
            assertThat(producer.history()).hasSize(1);
            assertThat(producer.commitCount()).isEqualTo(1);
        }

        @Test
        void publishAfterTheContextStartedWaitingForAcknowledgementsIsRejected() {
            // given
            AtomicReference<@Nullable CompletableFuture<Void>> late = new AtomicReference<>();

            // when
            UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(ctx -> {
                ctx.runOnAfterCommit(c -> late.set(awaitingAcks.publish(c, parcel("late"))));
                return awaitingAcks.publish(ctx, parcel("p-1"));
            }).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(late.get()).failsWithin(Duration.ofSeconds(5))
                                  .withThrowableThat()
                                  .havingRootCause()
                                  .isInstanceOf(IllegalStateException.class);
            assertThat(clientFactory.<byte[]>producers().getFirst().history()).hasSize(1);
        }

        @Test
        void failedSendOutsideAContextAbortsAndReturnsTheProducer() {
            // given
            transactional.publish(null, parcel("warm-up")).orTimeout(30, TimeUnit.SECONDS).join();
            MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
            producer.sendException = new RecordTooLargeException("too large");

            // when
            CompletableFuture<Void> result = transactional.publish(null, parcel("p-1"));

            // then
            assertPublicationFailure(result, "too large");
            assertThat(producer.transactionAborted()).isTrue();

            // when
            producer.sendException = null;
            transactional.publish(null, parcel("p-2")).orTimeout(30, TimeUnit.SECONDS).join();

            // then - the same producer ran the next transaction
            assertThat(clientFactory.producers()).hasSize(1);
            assertThat(producer.commitCount()).isEqualTo(2);
        }

        @Test
        void fencedProducerIsDiscardedAndItsIdentifierReused() {
            // given
            transactional.publish(null, parcel("warm-up")).orTimeout(30, TimeUnit.SECONDS).join();
            MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
            producer.commitTransactionException = new ProducerFencedException("fenced");

            // when
            CompletableFuture<Void> result = transactional.publish(null, parcel("p-1"));

            // then
            assertPublicationFailure(result, "fenced");
            assertThat(producer.closed()).isTrue();

            // when
            transactional.publish(null, parcel("p-2")).orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(clientFactory.producers()).hasSize(2);
            assertThat(clientFactory.producerProperties().get(1))
                    .containsEntry(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "parcels-1-0");
        }

        @Test
        void commitTimeoutFailsTheContextAndDiscardsTheProducerWithoutAborting() {
            // given
            transactional.publish(null, parcel("warm-up")).orTimeout(30, TimeUnit.SECONDS).join();
            MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
            producer.commitTransactionException =
                    new org.apache.kafka.common.errors.TimeoutException("commit timed out");

            // when
            CompletableFuture<Void> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> transactional.publish(ctx, parcel("p-1"))
            );

            // then - the transaction may still commit on the broker, so Kafka allows no abort: the producer is closed
            assertPublicationFailure(result, "commit timed out");
            assertThat(producer.transactionAborted()).isFalse();
            assertThat(producer.closed()).isTrue();

            // when
            UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(ctx -> transactional.publish(ctx, parcel("p-2")))
                               .orTimeout(5, TimeUnit.SECONDS).join();

            // then - a fresh producer, with the discarded producer's identifier, publishes the next context
            assertThat(clientFactory.producers()).hasSize(2);
            MockProducer<String, byte[]> replacement = clientFactory.<byte[]>producers().get(1);
            assertThat(replacement.transactionCommitted()).isTrue();
            assertThat(replacement.history()).singleElement()
                                             .satisfies(r -> assertThat(new String(r.value(), StandardCharsets.UTF_8))
                                                     .contains("p-2"));
            assertThat(clientFactory.producerProperties().get(1))
                    .containsEntry(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "parcels-1-0");
        }

        @Test
        void failingToInitializeTransactionsIsReportedAsPublicationFailure() {
            // given
            clientFactory.prepareProducers(
                    producer -> producer.initTransactionException = new KafkaException("init failed")
            );

            // when
            CompletableFuture<Void> result = transactional.publish(null, parcel("p-1"));

            // then
            assertPublicationFailure(result, "init failed");
            assertThat(clientFactory.producers()).singleElement().satisfies(p -> assertThat(p.closed()).isTrue());
        }

        @Test
        void failingToBeginATransactionIsReportedAsPublicationFailure() {
            // given
            transactional.publish(null, parcel("warm-up")).orTimeout(30, TimeUnit.SECONDS).join();
            MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
            producer.beginTransactionException = new KafkaException("begin failed");

            // when
            CompletableFuture<Void> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> transactional.publish(ctx, parcel("p-1"))
            );

            // then
            assertPublicationFailure(result, "begin failed");
            assertThat(producer.closed()).isTrue();
        }

        @Test
        void publishAfterBeginningTheTransactionFailedSaysSo() {
            // given
            transactional.publish(null, parcel("warm-up")).orTimeout(30, TimeUnit.SECONDS).join();
            MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
            producer.beginTransactionException = new KafkaException("begin failed");
            AtomicReference<@Nullable CompletableFuture<Void>> second = new AtomicReference<>();

            // when
            UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(ctx -> {
                CompletableFuture<Void> first = transactional.publish(ctx, parcel("p-1"));
                second.set(transactional.publish(ctx, parcel("p-2")));
                return first;
            }).exceptionally(e -> null).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(second.get()).failsWithin(Duration.ofSeconds(5))
                                    .withThrowableThat()
                                    .havingRootCause()
                                    .isInstanceOf(IllegalStateException.class)
                                    .withMessageContaining("could not be begun");
        }

        @Test
        void failingToCreateAProducerIsReportedAsPublicationFailureInBothConfirmingModes() {
            // given
            clientFactory.failCreationWith(new KafkaException("no producer"));

            // when
            CompletableFuture<Void> transactionalResult = transactional.publish(null, parcel("p-1"));
            CompletableFuture<Void> acknowledgedResult = awaitingAcks.publish(null, parcel("p-1"));

            // then
            assertPublicationFailure(transactionalResult, "no producer");
            assertPublicationFailure(acknowledgedResult, "no producer");
        }

        @Test
        void entitlementIsClaimedWhenSentWithoutConfirmation() {
            // given
            RecordingEntitlementManager entitlementManager = new RecordingEntitlementManager();
            KafkaEventSink testSubject = new KafkaEventSink(
                    clientFactory, baseConfiguration.withConfirmationMode(ConfirmationMode.NONE), entitlementManager
            );

            // when
            testSubject.publish(null, parcel("p-1"), parcel("p-2")).orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(entitlementManager.claims()).singleElement()
                                                   .extracting(RecordingEntitlementManager.Claim::count)
                                                   .isEqualTo(2);
        }

        @Test
        void entitlementIsClaimedOnlyForAcknowledgedEvents() {
            // given
            RecordingKafkaClientFactory unacknowledging = new RecordingKafkaClientFactory(false);
            RecordingEntitlementManager entitlementManager = new RecordingEntitlementManager();
            KafkaEventSink testSubject = new KafkaEventSink(
                    unacknowledging, baseConfiguration.withAckTimeout(Duration.ofMillis(100)), entitlementManager
            );

            // when
            CompletableFuture<Void> unacknowledged = testSubject.publish(null, parcel("lost"));
            CompletableFuture<Void> acknowledged = testSubject.publish(null, parcel("p-1"), parcel("p-2"));
            MockProducer<String, byte[]> producer = unacknowledging.<byte[]>producers().getFirst();
            producer.errorNext(new KafkaException("lost"));
            producer.completeNext();
            producer.completeNext();

            // then
            assertThat(unacknowledged).failsWithin(Duration.ofSeconds(5));
            assertThat(acknowledged).succeedsWithin(Duration.ofSeconds(5));
            assertThat(entitlementManager.claims()).singleElement()
                                                   .extracting(RecordingEntitlementManager.Claim::count)
                                                   .isEqualTo(2);
        }

        @Test
        void entitlementIsClaimedOnlyForCommittedTransactions() {
            // given
            RecordingEntitlementManager entitlementManager = new RecordingEntitlementManager();
            KafkaEventSink testSubject = new KafkaEventSink(
                    clientFactory,
                    baseConfiguration.withConfirmationMode(ConfirmationMode.TRANSACTIONAL)
                                     .withTransactionalIdPrefix("parcels-2-"),
                    entitlementManager
            );

            // when
            UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(ctx -> {
                testSubject.publish(ctx, parcel("aborted"));
                return CompletableFuture.failedFuture(new IllegalStateException("handler failed"));
            }).exceptionally(e -> null).orTimeout(5, TimeUnit.SECONDS).join();
            testSubject.publish(null, parcel("p-1"), parcel("p-2")).orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(entitlementManager.claims()).singleElement()
                                                   .extracting(RecordingEntitlementManager.Claim::count)
                                                   .isEqualTo(2);
        }
    }

    @Nested
    class FailingEntitlementClaims {

        private final RecordingKafkaClientFactory clientFactory = new RecordingKafkaClientFactory(true);
        private final RecordingEntitlementManager entitlementManager = new RecordingEntitlementManager();

        @BeforeEach
        void failClaims() {
            entitlementManager.failClaimsWith(new IllegalStateException("throughput exceeded"));
        }

        private KafkaEventSink sink(KafkaEventSinkConfiguration configuration) {
            return new KafkaEventSink(clientFactory, configuration, entitlementManager);
        }

        @Test
        void withoutConfirmationPublicationSucceedsAndTheProducerStaysUsable() {
            // given
            KafkaEventSink testSubject = sink(baseConfiguration.withConfirmationMode(ConfirmationMode.NONE));

            // when
            CompletableFuture<Void> first = testSubject.publish(null, parcel("p-1"));
            CompletableFuture<Void> second = testSubject.publish(null, parcel("p-2"));

            // then
            assertThat(first).succeedsWithin(Duration.ofSeconds(5));
            assertThat(second).succeedsWithin(Duration.ofSeconds(5));
            assertThat(clientFactory.producers()).hasSize(1);
            assertThat(clientFactory.<byte[]>producers().getFirst().history()).hasSize(2);
            assertThat(entitlementManager.claims()).hasSize(2);
        }

        @Test
        void waitingForAcknowledgementPublicationSucceedsAndTheProducerStaysUsable() {
            // given
            KafkaEventSink testSubject = sink(baseConfiguration.withConfirmationMode(ConfirmationMode.WAIT_FOR_ACK));

            // when
            CompletableFuture<Void> outsideContext = testSubject.publish(null, parcel("p-1"));
            CompletableFuture<Void> inContext = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-2"))
            );

            // then
            assertThat(outsideContext).succeedsWithin(Duration.ofSeconds(5));
            assertThat(inContext).succeedsWithin(Duration.ofSeconds(5));
            assertThat(clientFactory.producers()).hasSize(1);
            assertThat(clientFactory.<byte[]>producers().getFirst().history()).hasSize(2);
            assertThat(entitlementManager.claims()).hasSize(2);
        }

        @Test
        void transactionalPublicationSucceedsAndTheReleasedProducerStaysUsable() {
            // given
            KafkaEventSink testSubject = sink(baseConfiguration.withConfirmationMode(ConfirmationMode.TRANSACTIONAL)
                                                               .withTransactionalIdPrefix("parcels-1-"));

            // when
            CompletableFuture<Void> inContext = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-1"))
            );

            // then - the released producer is neither aborted nor closed
            assertThat(inContext).succeedsWithin(Duration.ofSeconds(5));
            MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();
            assertThat(producer.transactionCommitted()).isTrue();
            assertThat(producer.transactionAborted()).isFalse();
            assertThat(producer.closed()).isFalse();

            // when - the pool hands out the same producer for the next transactions
            CompletableFuture<Void> outsideContext = testSubject.publish(null, parcel("p-2"));
            CompletableFuture<Void> nextContext = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-3"))
            );

            // then
            assertThat(outsideContext).succeedsWithin(Duration.ofSeconds(5));
            assertThat(nextContext).succeedsWithin(Duration.ofSeconds(5));
            assertThat(clientFactory.producers()).hasSize(1);
            assertThat(producer.transactionAborted()).isFalse();
            assertThat(producer.history()).hasSize(3);
            assertThat(entitlementManager.claims()).hasSize(3);
        }
    }

    @Nested
    class Shutdown {

        private final RecordingKafkaClientFactory clientFactory = new RecordingKafkaClientFactory(true);

        @Test
        void closesProducersAndRejectsLaterPublications() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(clientFactory, baseConfiguration);
            testSubject.publish(null, parcel("p-1")).orTimeout(30, TimeUnit.SECONDS).join();

            // when
            testSubject.shutdown().orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(clientFactory.producers().getFirst().closed()).isTrue();
            assertThat(testSubject.publish(null, parcel("p-2"))).isCompletedExceptionally();
        }

        @Test
        void publicationWhileTheProducersAreClosingIsRejected() {
            // given
            CompletableFuture<Void> closing = new CompletableFuture<>();
            clientFactory.holdUpClosingUntil(closing);
            KafkaEventSink testSubject = new KafkaEventSink(clientFactory, baseConfiguration);
            testSubject.publish(null, parcel("p-1")).orTimeout(30, TimeUnit.SECONDS).join();
            MockProducer<String, byte[]> producer = clientFactory.<byte[]>producers().getFirst();

            // when - the shared producer is still being closed
            CompletableFuture<Void> shutdown = testSubject.shutdown();
            CompletableFuture<Void> result = testSubject.publish(null, parcel("p-2"));
            closing.complete(null);

            // then
            assertThat(result).failsWithin(Duration.ofSeconds(5))
                              .withThrowableThat()
                              .havingRootCause()
                              .isInstanceOf(IllegalStateException.class)
                              .withMessageContaining("shut down");
            shutdown.orTimeout(30, TimeUnit.SECONDS).join();
            assertThat(producer.history()).hasSize(1);
        }

        @Test
        void closesIdlePooledProducers() {
            // given
            KafkaEventSink testSubject = new KafkaEventSink(
                    clientFactory,
                    baseConfiguration.withConfirmationMode(ConfirmationMode.TRANSACTIONAL)
                                     .withTransactionalIdPrefix("p-")
            );
            testSubject.publish(null, parcel("p-1")).orTimeout(30, TimeUnit.SECONDS).join();

            // when
            testSubject.shutdown().orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(clientFactory.producers().getFirst().closed()).isTrue();
            assertThat(testSubject.publish(null, parcel("p-2"))).isCompletedExceptionally();
        }
    }
}
