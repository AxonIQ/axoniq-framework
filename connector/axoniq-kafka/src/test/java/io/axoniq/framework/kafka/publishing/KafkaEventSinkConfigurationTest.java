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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.*;

class KafkaEventSinkConfigurationTest {

    private final KafkaEventSinkConfiguration defaults =
            KafkaEventSinkConfiguration.create(CodecTestFixtures.defaultCodec(), TopicResolver.fixed("parcels"));

    @Nested
    class Validation {

        @Test
        void rejectsMissingSettings() {
            // when / then
            assertThatThrownBy(() -> KafkaEventSinkConfiguration.create(null, TopicResolver.fixed("parcels")))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("codec");
            assertThatThrownBy(() -> KafkaEventSinkConfiguration.create(CodecTestFixtures.defaultCodec(), null))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("topicResolver");
            assertThatThrownBy(() -> defaults.withRecordKeyResolver(null))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("recordKeyResolver");
            assertThatThrownBy(() -> defaults.withConfirmationMode(null))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("confirmationMode");
            assertThatThrownBy(() -> defaults.withAckTimeout(null))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("ackTimeout");
            assertThatThrownBy(() -> defaults.withTransactionalIdPrefix(null))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("transactionalIdPrefix");
            assertThatThrownBy(() -> defaults.withProducerProperties(null))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("producerProperties");
            assertThatThrownBy(() -> defaults.withCommitExecutor(null))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("commitExecutor");
        }

        @Test
        void rejectsANonPositiveAcknowledgementTimeout() {
            // when / then
            assertThatThrownBy(() -> defaults.withAckTimeout(Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ackTimeout");
            assertThatThrownBy(() -> defaults.withAckTimeout(Duration.ofSeconds(-1)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ackTimeout");
        }

        @Test
        void rejectsABlankTransactionalIdentifierPrefix() {
            // when / then
            assertThatThrownBy(() -> defaults.withTransactionalIdPrefix(" "))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("transactionalIdPrefix");
        }

        @Test
        void rejectsATransactionalIdentifierInTheProducerProperties() {
            // when / then - the sink sets the transactional identifiers of its producers itself
            assertThatThrownBy(() -> defaults.withProducerProperties(
                    Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "parcels-1")
            )).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("transactional.id");
        }
    }

    @Nested
    class Copies {

        @Test
        void withMethodsLeaveTheOriginalUnchanged() {
            // given
            Executor executor = Runnable::run;

            // when
            KafkaEventSinkConfiguration changed = defaults.withConfirmationMode(ConfirmationMode.TRANSACTIONAL)
                                                          .withTransactionalIdPrefix("parcels-1-")
                                                          .withCommitExecutor(executor)
                                                          .withProducerProperties(Map.of("linger.ms", 5));

            // then
            assertThat(defaults.confirmationMode()).isEqualTo(ConfirmationMode.WAIT_FOR_ACK);
            assertThat(defaults.transactionalIdPrefix()).isNull();
            assertThat(defaults.commitExecutor()).isNull();
            assertThat(changed.confirmationMode()).isEqualTo(ConfirmationMode.TRANSACTIONAL);
            assertThat(changed.transactionalIdPrefix()).isEqualTo("parcels-1-");
            assertThat(changed.commitExecutor()).isSameAs(executor);
            assertThat(changed.producerProperties()).containsEntry("linger.ms", 5);
        }
    }
}
