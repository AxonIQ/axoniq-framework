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

package org.axonframework.deadline.dbscheduler;

import com.github.kagkarlsson.scheduler.serializer.GsonSerializer;
import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import com.github.kagkarlsson.scheduler.serializer.JavaSerializer;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.deadline.TestScopeDescriptor;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link DbSchedulerBinaryDeadlineDetails}.
 */
class DbSchedulerBinaryDeadlineDetailsTest {

    private static final String TEST_DEADLINE_NAME = "deadline-name";
    private static final String TEST_DEADLINE_PAYLOAD = "deadline-payload";
    private static final Metadata METADATA = Metadata.from(Map.of("someStringValue", "foo", "someIntValue", "2"));

    private final StoredDeadlineConverter converter = new StoredDeadlineConverter(new JacksonConverter());

    static List<Serializer> dbSchedulerSerializers() {
        return List.of(new JavaSerializer(), new JacksonSerializer(), new GsonSerializer());
    }

    @MethodSource("dbSchedulerSerializers")
    @ParameterizedTest
    void shouldBeSerializableByDbScheduler(Serializer serializer) {
        // given
        DbSchedulerBinaryDeadlineDetails expected = new DbSchedulerBinaryDeadlineDetails(
                "deadlineName",
                "someScope".getBytes(),
                "org.axonframework.modelling.command.AggregateScopeDescriptor",
                "{\"foo\":\"bar\"}".getBytes(),
                "com.someCompany.api.ImportantEvent",
                "1",
                "{\"traceId\":\"1acc25e2-58a1-4dec-8b43-55388188500a\"}".getBytes()
        );

        // when
        byte[] serialized = serializer.serialize(expected);
        DbSchedulerBinaryDeadlineDetails result =
                serializer.deserialize(DbSchedulerBinaryDeadlineDetails.class, serialized);

        // then
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void whenDataInPojoIsConvertedAndConvertedBackItShouldBeTheSame() {
        // given
        ScopeDescriptor descriptor = new TestScopeDescriptor("aggregateType", "identifier");
        DeadlineMessage message = new GenericDeadlineMessage(
                TEST_DEADLINE_NAME,
                new GenericMessage(new MessageType(String.class), TEST_DEADLINE_PAYLOAD, METADATA),
                Instant::now
        );

        // when
        DbSchedulerBinaryDeadlineDetails result =
                DbSchedulerBinaryDeadlineDetails.serialized(TEST_DEADLINE_NAME, descriptor, message, converter);

        // then
        assertThat(result.getD()).isEqualTo(TEST_DEADLINE_NAME);
        assertThat(result.getDeserializedScopeDescriptor(converter)).isEqualTo(descriptor);
        DeadlineMessage resultMessage = result.asDeadLineMessage(converter);
        assertThat(resultMessage.getDeadlineName()).isEqualTo(TEST_DEADLINE_NAME);
        assertThat(resultMessage.payload()).isEqualTo(TEST_DEADLINE_PAYLOAD);
        assertThat(resultMessage.metadata()).isEqualTo(METADATA);
    }

    @Test
    void aDeadlineWithoutPayloadIsConvertedBackWithoutPayload() {
        // given
        ScopeDescriptor descriptor = new TestScopeDescriptor("aggregateType", "identifier");
        DeadlineMessage message = new GenericDeadlineMessage(TEST_DEADLINE_NAME, new MessageType("none"), null);

        // when
        DbSchedulerBinaryDeadlineDetails result =
                DbSchedulerBinaryDeadlineDetails.serialized(TEST_DEADLINE_NAME, descriptor, message, converter);

        // then
        assertThat(result.asDeadLineMessage(converter).payload()).isNull();
    }
}
