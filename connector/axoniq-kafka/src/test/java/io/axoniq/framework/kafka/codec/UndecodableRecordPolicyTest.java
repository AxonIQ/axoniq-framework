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

package io.axoniq.framework.kafka.codec;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static io.axoniq.framework.kafka.utils.ConsumedRecords.foreign;
import static org.assertj.core.api.Assertions.*;

class UndecodableRecordPolicyTest {

    private final ConsumerRecord<String, byte[]> record = foreign("parcels", 2, 17, "key", "not an event".getBytes());
    private final KafkaEventCodec<byte[]> carryingNoEvent =
            CodecTestFixtures.decodingWith(CodecTestFixtures.defaultCodec(), r -> Optional.empty());

    @Nested
    class Decode {

        @Test
        void returnsTheDecodedRecordWithoutApplyingThePolicy() {
            // given
            DecodedRecord expected = new DecodedRecord(EventTestUtils.createEvent(0), Context.empty());
            KafkaEventCodec<byte[]> codec =
                    CodecTestFixtures.decodingWith(CodecTestFixtures.defaultCodec(), r -> Optional.of(expected));

            // when
            Optional<DecodedRecord> decoded = UndecodableRecordPolicy.FAIL.decode(codec, record);

            // then
            assertThat(decoded).contains(expected);
        }
    }

    @Nested
    class SkipAndLog {

        private Logger policyLogger;
        private ListAppender appender;

        @BeforeEach
        void attachAppender() {
            policyLogger = (Logger) LogManager.getLogger(UndecodableRecordPolicy.class);
            appender = new ListAppender("UndecodableRecordPolicy");
            appender.start();
            policyLogger.addAppender(appender);
        }

        @AfterEach
        void detachAppender() {
            policyLogger.removeAppender(appender);
            appender.stop();
        }

        @Test
        void logsWarningNamingTheRecordPosition() {
            // when
            Optional<DecodedRecord> decoded = UndecodableRecordPolicy.SKIP_AND_LOG.decode(carryingNoEvent, record);

            // then
            assertThat(decoded).isEmpty();
            assertThat(appender.getEvents()).singleElement().satisfies(event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getMessage().getFormattedMessage())
                        .contains("[17]").contains("[2]").contains("[parcels]");
            });
        }

        @Test
        void logsTheCauseOfAMalformedRecord() {
            // given
            UndecodableRecordException cause = new UndecodableRecordException("bad timestamp");

            // when
            Optional<DecodedRecord> decoded = UndecodableRecordPolicy.SKIP_AND_LOG.decode(
                    CodecTestFixtures.failingAt(CodecTestFixtures.defaultCodec(), 17, cause), record
            );

            // then
            assertThat(decoded).isEmpty();
            assertThat(appender.getEvents()).singleElement().satisfies(event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getMessage().getFormattedMessage()).contains("[17]").contains("cannot be decoded");
                assertThat(event.getThrown()).isSameAs(cause);
            });
        }
    }

    @Nested
    class Fail {

        @Test
        void failRejectsTheRecord() {
            // when / then
            assertThatThrownBy(() -> UndecodableRecordPolicy.FAIL.decode(carryingNoEvent, record))
                    .isInstanceOf(UndecodableRecordException.class)
                    .hasMessageContaining("[17]")
                    .hasMessageContaining("[parcels]");
        }

        @Test
        void failRejectsAMalformedRecordKeepingTheCause() {
            // given
            IllegalStateException cause = new IllegalStateException("cannot deserialize");

            // when / then
            assertThatThrownBy(() -> UndecodableRecordPolicy.FAIL.handle(
                    new TopicPartition("parcels", 2), 17, cause))
                    .isInstanceOf(UndecodableRecordException.class)
                    .hasMessageContaining("[17]")
                    .hasCause(cause);
        }
    }
}
