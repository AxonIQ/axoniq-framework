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
import io.axoniq.framework.kafka.common.TopicSubscription;
import org.junit.jupiter.api.*;

import java.time.Duration;

import static org.assertj.core.api.Assertions.*;

class KafkaStreamableEventSourceConfigurationTest {

    private final KafkaStreamableEventSourceConfiguration defaults = KafkaStreamableEventSourceConfiguration.create(
            CodecTestFixtures.defaultCodec(), TopicSubscription.topics("parcels")
    );

    @Test
    void rejectsMissingSettings() {
        // when / then
        assertThatThrownBy(() -> KafkaStreamableEventSourceConfiguration.create(null,
                                                                                TopicSubscription.topics("parcels")))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("codec");
        assertThatThrownBy(() -> KafkaStreamableEventSourceConfiguration.create(CodecTestFixtures.defaultCodec(),
                                                                                null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("subscription");
        assertThatThrownBy(() -> defaults.withUndecodableRecordPolicy(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("undecodableRecordPolicy");
        assertThatThrownBy(() -> defaults.withRetentionGapPolicy(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("retentionGapPolicy");
        assertThatThrownBy(() -> defaults.withPollTimeout(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("pollTimeout");
        assertThatThrownBy(() -> defaults.withMetadataRefreshInterval(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("metadataRefreshInterval");
        assertThatThrownBy(() -> defaults.withConsumerProperties(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("consumerProperties");
    }

    @Test
    void rejectsABufferSizeBelowOne() {
        // when / then
        assertThatThrownBy(() -> defaults.withBufferSize(0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bufferSize");
    }

    @Test
    void rejectsNonPositiveDurations() {
        // when / then
        assertThatThrownBy(() -> defaults.withPollTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("pollTimeout");
        assertThatThrownBy(() -> defaults.withMetadataRefreshInterval(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("metadataRefreshInterval");
    }
}
