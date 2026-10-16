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

package io.axoniq.framework.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.axonframework.messaging.core.Context;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

class KafkaResourcesTest {

    @Test
    void describesRecordPositionKeyAndTimestamp() {
        // given
        Instant timestamp = Instant.parse("2026-10-10T12:00:00Z");
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "parcels", 4, 99, timestamp.toEpochMilli(), TimestampType.CREATE_TIME, -1, -1,
                "parcel-1", "value", new RecordHeaders(), Optional.empty()
        );

        // when
        Context result = KafkaResources.addToContext(Context.empty(), record);

        // then
        assertThat(result.getResource(KafkaResources.TOPIC_PARTITION)).isEqualTo(new TopicPartition("parcels", 4));
        assertThat(result.getResource(KafkaResources.OFFSET)).isEqualTo(99L);
        assertThat(result.getResource(KafkaResources.RECORD_KEY)).isEqualTo("parcel-1");
        assertThat(result.getResource(KafkaResources.RECORD_TIMESTAMP)).isEqualTo(timestamp);
    }

    @Test
    void omitsKeyAndTimestampWhenRecordHasNone() {
        // given
        ConsumerRecord<String, String> record = new ConsumerRecord<>("parcels", 0, 1, null, "value");

        // when
        Context result = KafkaResources.addToContext(Context.empty(), record);

        // then
        assertThat(result.containsResource(KafkaResources.RECORD_KEY)).isFalse();
        assertThat(result.containsResource(KafkaResources.RECORD_TIMESTAMP)).isFalse();
        assertThat(result.containsResource(KafkaResources.TOPIC_PARTITION)).isTrue();
    }

    @Test
    void keepsExistingResources() {
        // given
        Context.ResourceKey<String> other = Context.ResourceKey.withLabel("other");
        Context context = Context.with(other, "kept");

        // when
        Context result = KafkaResources.addToContext(context, new ConsumerRecord<>("parcels", 0, 1, null, "v"));

        // then
        assertThat(result.getResource(other)).isEqualTo("kept");
    }
}
