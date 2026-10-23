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
import io.axoniq.framework.kafka.utils.KafkaTestBroker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

class KafkaEventSinkIT {

    private static final Duration READ_TIME = Duration.ofSeconds(3);

    private final DefaultKafkaEventCodec codec = CodecTestFixtures.defaultCodec();
    private @Nullable KafkaEventSink testSubject;

    record ParcelDispatched(String parcelId) {

    }

    private static EventMessage parcel(String parcelId) {
        return EventTestUtils.asEventMessage(new ParcelDispatched(parcelId));
    }

    @AfterEach
    void shutdown() {
        if (testSubject != null) {
            testSubject.shutdown().orTimeout(30, TimeUnit.SECONDS).join();
        }
    }

    @Nested
    class Transactional {

        private String topic;

        @BeforeEach
        void setUp() {
            topic = KafkaTestBroker.createTopic(KafkaTestBroker.uniqueTopic("transactional"), 2);
            testSubject = new KafkaEventSink(
                    KafkaTestBroker.clientFactory(),
                    KafkaEventSinkConfiguration.create(codec, TopicResolver.fixed(topic))
                                               .withConfirmationMode(ConfirmationMode.TRANSACTIONAL)
                                               .withTransactionalIdPrefix(topic + "-")
            );
        }

        @Test
        void readCommittedConsumerSeesOnlyEventsOfCommittedContexts() {
            // when
            UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("committed-1"), parcel("committed-2"))
            ).orTimeout(30, TimeUnit.SECONDS).join();
            CompletableFuture<Object> rolledBack = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("rolled-back"))
                                      .thenCompose(r -> CompletableFuture.failedFuture(
                                              new IllegalStateException("boom")
                                      ))
            );
            assertThat(rolledBack).failsWithin(Duration.ofSeconds(30));

            // then
            List<ConsumerRecord<String, byte[]>> committed =
                    KafkaTestBroker.readAll(topic, "read_committed", READ_TIME);
            assertThat(committed).extracting(r -> codec.decode(r).orElseThrow().event()
                                                       .payloadAs(ParcelDispatched.class).parcelId())
                                 .containsExactlyInAnyOrder("committed-1", "committed-2");
        }

        @Test
        void eventsOfAContextAreInvisibleUntilItCommits() {
            // given
            CompletableFuture<Void> release = new CompletableFuture<>();
            CompletableFuture<Void> published = new CompletableFuture<>();

            // when
            CompletableFuture<Void> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("pending"))
                                      .whenComplete((r, e) -> published.complete(null))
                                      .thenCompose(r -> release)
            );
            published.orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(KafkaTestBroker.readAll(topic, "read_committed", READ_TIME)).isEmpty();

            // when
            release.complete(null);
            result.orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(KafkaTestBroker.readAll(topic, "read_committed", READ_TIME)).hasSize(1);
        }
    }

    @Nested
    class WaitingForAcknowledgement {

        @Test
        void failedDeliveryFailsTheContext() {
            // given - topics are not created automatically, so publishing to a missing topic fails
            String missingTopic = KafkaTestBroker.uniqueTopic("missing");
            testSubject = new KafkaEventSink(
                    KafkaTestBroker.clientFactory(),
                    KafkaEventSinkConfiguration.create(codec, TopicResolver.fixed(missingTopic))
                                               .withProducerProperties(Map.of(ProducerConfig.MAX_BLOCK_MS_CONFIG, 500))
            );

            // when
            CompletableFuture<Void> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> testSubject.publish(ctx, parcel("p-1"))
            );

            // then
            assertThat(result).failsWithin(Duration.ofSeconds(30));
        }

        @Test
        void acknowledgedEventsAreReadable() {
            // given
            String topic = KafkaTestBroker.createTopic(KafkaTestBroker.uniqueTopic("acked"), 1);
            testSubject = new KafkaEventSink(KafkaTestBroker.clientFactory(),
                                             KafkaEventSinkConfiguration.create(codec, TopicResolver.fixed(topic)));

            // when
            UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(ctx -> testSubject.publish(ctx, parcel("p-1")))
                               .orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(KafkaTestBroker.readAll(topic, "read_uncommitted", READ_TIME)).hasSize(1);
        }
    }
}
