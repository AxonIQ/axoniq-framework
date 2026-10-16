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

package io.axoniq.framework.kafka.streaming;

import io.axoniq.framework.kafka.codec.CodecTestFixtures;
import io.axoniq.framework.kafka.codec.DefaultKafkaEventCodec;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.framework.kafka.utils.RecordingEntitlementManager;
import io.axoniq.framework.kafka.utils.RecordingKafkaClientFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

class KafkaStreamableEventSourceTest {

    private final DefaultKafkaEventCodec codec = CodecTestFixtures.defaultCodec();
    private final RecordingKafkaClientFactory clientFactory = new RecordingKafkaClientFactory(true);
    private @Nullable KafkaStreamableEventSource testSubject;

    @AfterEach
    void tearDown() {
        if (testSubject != null) {
            testSubject.shutdown().orTimeout(15, TimeUnit.SECONDS).join();
        }
    }

    private KafkaStreamableEventSource source(Map<String, Object> consumerProperties) {
        return new KafkaStreamableEventSource(
                clientFactory,
                KafkaStreamableEventSourceConfiguration.create(codec, TopicSubscription.topics("parcels"))
                                                      .withPollTimeout(Duration.ofMillis(10))
                                                      .withConsumerProperties(consumerProperties),
                new RecordingEntitlementManager()
        );
    }

    @Nested
    class IsolationLevel {

        @Test
        void consumersReadCommittedRecordsByDefault() {
            // given
            testSubject = source(Map.of());

            // when
            testSubject.open(StreamingCondition.startingFrom(null), null).close();

            // then
            assertThat(clientFactory.consumerProperties())
                    .singleElement()
                    .satisfies(properties -> assertThat(properties)
                            .containsEntry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"));
        }

        @Test
        void isolationLevelOfTheSourcesConsumerPropertiesWins() {
            // given
            testSubject = source(Map.of(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted"));

            // when
            testSubject.open(StreamingCondition.startingFrom(null), null).close();

            // then
            assertThat(clientFactory.consumerProperties())
                    .singleElement()
                    .satisfies(properties -> assertThat(properties)
                            .containsEntry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted"));
        }
    }

    @Nested
    class Opening {

        @Test
        void rejectsATokenOfAnotherType() {
            // given
            testSubject = source(Map.of());

            // when / then
            assertThatThrownBy(() -> testSubject.open(
                    StreamingCondition.startingFrom(new GlobalSequenceTrackingToken(3)), null
            )).isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining(GlobalSequenceTrackingToken.class.getName());
            assertThat(clientFactory.consumerProperties()).isEmpty();
        }

        @Test
        void failureToCreateTheConsumerFailsTheStream() {
            // given
            testSubject = source(Map.of());
            clientFactory.failConsumerCreationWith(
                    new KafkaException("Failed to construct kafka consumer", new ConfigException("bad settings"))
            );

            // when
            MessageStream<EventMessage> stream = testSubject.open(StreamingCondition.startingFrom(null), null);

            // then
            assertThat(stream.error()).get(InstanceOfAssertFactories.THROWABLE)
                                      .hasRootCauseInstanceOf(ConfigException.class);
        }

        @Test
        void streamOpenedAfterShutdownFails() {
            // given
            testSubject = source(Map.of());
            testSubject.shutdown().orTimeout(15, TimeUnit.SECONDS).join();

            // when
            MessageStream<EventMessage> stream = testSubject.open(StreamingCondition.startingFrom(null), null);

            // then
            assertThat(stream.error()).get(InstanceOfAssertFactories.THROWABLE)
                                      .isInstanceOf(IllegalStateException.class)
                                      .hasMessageContaining("shut down");
            assertThat(clientFactory.consumerProperties()).isEmpty();
        }

        @Test
        void tokenQueriesAfterShutdownFailWithoutCreatingAMetadataClient() {
            // given
            testSubject = source(Map.of());
            testSubject.shutdown().orTimeout(15, TimeUnit.SECONDS).join();

            // when
            CompletableFuture<TrackingToken> result = testSubject.latestToken(null);

            // then
            assertThat(result).failsWithin(Duration.ofSeconds(5))
                              .withThrowableThat()
                              .withRootCauseInstanceOf(IllegalStateException.class);
            assertThat(clientFactory.adminProperties()).isEmpty();
        }
    }

    @Nested
    class MetadataClient {

        @Test
        void usesTheConnectionSettingsOfTheSourcesConsumers() {
            // given
            testSubject = source(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092",
                                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL",
                                        CommonClientConfigs.CLIENT_ID_CONFIG, "shipping",
                                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50));

            // when
            CompletableFuture<TrackingToken> result = testSubject.latestToken(null);

            // then - this factory provides no admin client, so the query fails after asking for one
            assertThat(result).failsWithin(Duration.ofSeconds(5));
            assertThat(clientFactory.adminProperties())
                    .singleElement()
                    .satisfies(properties -> assertThat(properties)
                            .containsEntry(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092")
                            .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
                            .containsEntry(CommonClientConfigs.CLIENT_ID_CONFIG, "shipping")
                            .doesNotContainKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        }
    }
}
