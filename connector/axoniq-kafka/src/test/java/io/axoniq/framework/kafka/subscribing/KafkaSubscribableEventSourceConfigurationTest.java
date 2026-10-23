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

package io.axoniq.framework.kafka.subscribing;

import io.axoniq.framework.kafka.codec.CodecTestFixtures;
import io.axoniq.framework.kafka.common.TopicSubscription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class KafkaSubscribableEventSourceConfigurationTest {

    private final KafkaSubscribableEventSourceConfiguration defaults =
            KafkaSubscribableEventSourceConfiguration.create(CodecTestFixtures.defaultCodec(),
                                                             TopicSubscription.topics("parcels"),
                                                             "billing");

    @Test
    void rejectsMissingSettings() {
        // when / then
        assertThatThrownBy(() -> KafkaSubscribableEventSourceConfiguration.create(
                null, TopicSubscription.topics("parcels"), "billing"
        )).isInstanceOf(NullPointerException.class).hasMessageContaining("codec");
        assertThatThrownBy(() -> KafkaSubscribableEventSourceConfiguration.create(
                CodecTestFixtures.defaultCodec(), null, "billing"
        )).isInstanceOf(NullPointerException.class).hasMessageContaining("subscription");
        assertThatThrownBy(() -> KafkaSubscribableEventSourceConfiguration.create(
                CodecTestFixtures.defaultCodec(), TopicSubscription.topics("parcels"), null
        )).isInstanceOf(NullPointerException.class).hasMessageContaining("groupId");
        assertThatThrownBy(() -> defaults.withFailedRecordPolicy(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("failedRecordPolicy");
        assertThatThrownBy(() -> defaults.withUndecodableRecordPolicy(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("undecodableRecordPolicy");
        assertThatThrownBy(() -> defaults.withInitialPosition(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("initialPosition");
        assertThatThrownBy(() -> defaults.withRetentionGapPolicy(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("retentionGapPolicy");
        assertThatThrownBy(() -> defaults.withDeadLetterTopicSuffix(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("deadLetterTopicSuffix");
        assertThatThrownBy(() -> defaults.withConsumerProperties(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("consumerProperties");
        assertThatThrownBy(() -> defaults.withProducerProperties(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("producerProperties");
    }

    @Test
    void rejectsABlankGroupIdentifier() {
        // when / then
        assertThatThrownBy(() -> KafkaSubscribableEventSourceConfiguration.create(
                CodecTestFixtures.defaultCodec(), TopicSubscription.topics("parcels"), " "
        )).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("groupId");
    }

    @Test
    void rejectsAConsumerCountBelowOne() {
        // when / then
        assertThatThrownBy(() -> defaults.withConsumerCount(0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("consumerCount");
    }

    @Test
    void rejectsAnEmptyDeadLetterTopicSuffix() {
        // when / then
        assertThatThrownBy(() -> defaults.withDeadLetterTopicSuffix(""))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("deadLetterTopicSuffix");
    }

    @Test
    void rejectsATransactionalIdentifierForTheDeadLetterProducer() {
        // when / then
        assertThatThrownBy(() -> defaults.withProducerProperties(
                Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "billing-dlt")
        )).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("transactional.id");
    }

    @Test
    void rejectsAnInitialRetryBackoffAboveTheMaximum() {
        // when / then
        assertThatThrownBy(() -> defaults.withRetryBackoff(Duration.ofMinutes(1), Duration.ofSeconds(30)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialBackoff")
                .hasMessageContaining("retryMaxBackoff");
    }

    @Test
    void rejectsNonPositiveDurations() {
        // when / then
        assertThatThrownBy(() -> defaults.withRetryBackoff(Duration.ZERO, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("retryInitialBackoff");
        assertThatThrownBy(() -> defaults.withRetryBackoff(Duration.ofSeconds(1), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("retryMaxBackoff");
        assertThatThrownBy(() -> defaults.withPollTimeout(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("pollTimeout");
        assertThatThrownBy(() -> defaults.withRevocationTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("revocationTimeout");
    }
}
