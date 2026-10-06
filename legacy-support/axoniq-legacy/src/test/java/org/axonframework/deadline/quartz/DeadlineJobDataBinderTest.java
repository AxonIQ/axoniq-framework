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

package org.axonframework.deadline.quartz;

import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.deadline.DeadlineException;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.deadline.TestScopeDescriptor;
import org.axonframework.deadline.UnknownDeadlinePayload;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.junit.jupiter.api.*;
import org.quartz.JobDataMap;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.axonframework.deadline.quartz.DeadlineJob.DeadlineJobDataBinder.*;

/**
 * Test class validating the {@link DeadlineJob.DeadlineJobDataBinder}, which keeps the job data layout of Axon
 * Framework 4.13.
 */
class DeadlineJobDataBinderTest {

    private static final String TEST_DEADLINE_NAME = "deadline-name";
    private static final String PAYLOAD = "deadline-payload";

    private final StoredDeadlineConverter converter = new StoredDeadlineConverter(new JacksonConverter());
    private final ScopeDescriptor scope = new TestScopeDescriptor("aggregate-type", "aggregate-identifier");
    private final Metadata metadata = Metadata.with("some-key", "some-value");
    private final DeadlineMessage deadlineMessage = new GenericDeadlineMessage(
            TEST_DEADLINE_NAME,
            new GenericMessage(new MessageType(String.class), PAYLOAD, metadata),
            () -> Instant.now().truncatedTo(ChronoUnit.MILLIS)
    );

    @Nested
    class ToJobData {

        @Test
        void writesTheKeysOfAxonFramework4() {
            // when
            JobDataMap result = toJobData(converter, deadlineMessage, scope);

            // then
            assertThat(result.getKeys()).containsExactlyInAnyOrder(
                    DEADLINE_NAME, MESSAGE_ID, MESSAGE_TIMESTAMP, SERIALIZED_MESSAGE_PAYLOAD, MESSAGE_TYPE,
                    MESSAGE_REVISION, MESSAGE_METADATA, SERIALIZED_DEADLINE_SCOPE, SERIALIZED_DEADLINE_SCOPE_CLASS_NAME
            );
            assertThat(result.get(DEADLINE_NAME)).isEqualTo(TEST_DEADLINE_NAME);
            assertThat(result.get(MESSAGE_ID)).isEqualTo(deadlineMessage.identifier());
            assertThat(result.get(MESSAGE_TIMESTAMP)).isEqualTo(deadlineMessage.timestamp().toString());
            assertThat(result.get(MESSAGE_TYPE)).isEqualTo(String.class.getName());
            assertThat(result.get(MESSAGE_REVISION)).isNull();
            assertThat(result.get(SERIALIZED_DEADLINE_SCOPE_CLASS_NAME)).isEqualTo(TestScopeDescriptor.class.getName());
            assertThat(result.get(SERIALIZED_MESSAGE_PAYLOAD)).isInstanceOf(byte[].class);
            assertThat(result.get(MESSAGE_METADATA)).isInstanceOf(byte[].class);
            assertThat(result.get(SERIALIZED_DEADLINE_SCOPE)).isInstanceOf(byte[].class);
        }

        @Test
        void storesADeadlineWithoutPayloadUnderTheEmptyType() {
            // given
            DeadlineMessage withoutPayload =
                    new GenericDeadlineMessage(TEST_DEADLINE_NAME, new MessageType("none"), null);

            // when
            JobDataMap result = toJobData(converter, withoutPayload, scope);

            // then
            assertThat(result.get(MESSAGE_TYPE)).isEqualTo(StoredDeadlineConverter.EMPTY_TYPE);
            assertThat(deadlineMessage(converter, result).payload()).isNull();
        }
    }

    @Nested
    class FromJobData {

        @Test
        void readsBackTheDeadlineMessageAndScope() {
            // given
            JobDataMap jobData = toJobData(converter, deadlineMessage, scope);

            // when
            DeadlineMessage result = deadlineMessage(converter, jobData);

            // then
            assertThat(result.getDeadlineName()).isEqualTo(TEST_DEADLINE_NAME);
            assertThat(result.identifier()).isEqualTo(deadlineMessage.identifier());
            assertThat(result.timestamp()).isEqualTo(deadlineMessage.timestamp());
            assertThat(result.payload()).isEqualTo(PAYLOAD);
            assertThat(result.type()).isEqualTo(StoredDeadlineConverter.messageTypeOf(PAYLOAD));
            assertThat(result.metadata()).isEqualTo(metadata);
            assertThat(deadlineScope(converter, jobData)).isEqualTo(scope);
        }

        @Test
        void readsATimestampStoredInEpochMillis() {
            // given
            JobDataMap jobData = toJobData(converter, deadlineMessage, scope);
            jobData.put(MESSAGE_TIMESTAMP, deadlineMessage.timestamp().toEpochMilli());

            // when
            DeadlineMessage result = deadlineMessage(converter, jobData);

            // then
            assertThat(result.timestamp()).isEqualTo(deadlineMessage.timestamp());
        }

        @Test
        void readsAnUnknownPayloadTypeAsUnknownDeadlinePayload() {
            // given
            JobDataMap jobData = toJobData(converter, deadlineMessage, scope);
            jobData.put(MESSAGE_TYPE, "com.example.RemovedPayload");

            // when
            DeadlineMessage result = deadlineMessage(converter, jobData);

            // then
            assertThat(result.payload()).isInstanceOfSatisfying(
                    UnknownDeadlinePayload.class,
                    unknown -> assertThat(unknown.typeName()).isEqualTo("com.example.RemovedPayload")
            );
        }

        @Test
        void rejectsAJobInTheAxonFramework33Layout() {
            // given
            JobDataMap jobData = new JobDataMap();
            jobData.put("serializedDeadlineMessage", new byte[]{1, 2, 3});
            jobData.put("serializedDeadlineMessageClassName", "org.axonframework.deadline.GenericDeadlineMessage");

            // when / then
            assertThatThrownBy(() -> deadlineMessage(converter, jobData))
                    .isInstanceOf(DeadlineException.class)
                    .hasMessageContaining("Axon Framework 3.3");
        }
    }
}
