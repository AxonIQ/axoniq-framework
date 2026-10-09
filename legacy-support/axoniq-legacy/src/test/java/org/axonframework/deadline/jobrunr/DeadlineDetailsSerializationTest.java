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

package org.axonframework.deadline.jobrunr;

import org.axonframework.common.TypeReference;
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

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the conversion of JobRunr's {@link DeadlineDetails}.
 */
class DeadlineDetailsSerializationTest {

    private static final String TEST_DEADLINE_NAME = "deadline-name";
    private static final String TEST_DEADLINE_PAYLOAD = "deadline-payload";

    private static final TypeReference<Map<String, Object>> FIELDS = new TypeReference<>() {
    };

    private final StoredDeadlineConverter converter = new StoredDeadlineConverter(new JacksonConverter());
    private final ScopeDescriptor descriptor = new TestScopeDescriptor("aggregateType", "identifier");
    private final Metadata metadata = Metadata.from(Map.of("someStringValue", "foo", "someIntValue", "2"));

    @Test
    void whenSerializedAndDeserializedAllPropertiesShouldBeTheSame() {
        // given
        DeadlineMessage message = new GenericDeadlineMessage(
                TEST_DEADLINE_NAME,
                new GenericMessage(new MessageType(String.class), TEST_DEADLINE_PAYLOAD, metadata),
                Instant::now
        );

        // when
        String serialized = DeadlineDetails.serialized(TEST_DEADLINE_NAME, descriptor, message, converter);
        DeadlineDetails result = converter.fromStored(serialized, DeadlineDetails.class);

        // then
        assertThat(result.getDeadlineName()).isEqualTo(TEST_DEADLINE_NAME);
        assertThat(result.getDeserializedScopeDescriptor(converter)).isEqualTo(descriptor);
        DeadlineMessage resultMessage = result.asDeadLineMessage(converter);
        assertThat(resultMessage.getDeadlineName()).isEqualTo(TEST_DEADLINE_NAME);
        assertThat(resultMessage.payload()).isEqualTo(TEST_DEADLINE_PAYLOAD);
        assertThat(resultMessage.metadata()).isEqualTo(metadata);
    }

    @Test
    void theStoredDetailsHaveTheFieldsOfAxonFramework4() {
        // given
        DeadlineMessage message = new GenericDeadlineMessage(
                TEST_DEADLINE_NAME,
                new GenericMessage(new MessageType(String.class), TEST_DEADLINE_PAYLOAD),
                Instant::now
        );

        // when
        String serialized = DeadlineDetails.serialized(TEST_DEADLINE_NAME, descriptor, message, converter);
        Map<String, Object> fields = new JacksonConverter().convert(serialized, FIELDS.getType());

        // then
        assertThat(fields).containsOnlyKeys("deadlineName", "scopeDescriptor", "scopeDescriptorClass", "payload",
                                            "payloadClass", "payloadRevision", "metaData");
        assertThat(fields.get("payloadClass")).isEqualTo(String.class.getName());
        assertThat(fields.get("payloadRevision")).isNull();
    }

    @Test
    void aDeadlineWithoutPayloadIsReadBackWithoutPayload() {
        // given
        DeadlineMessage message = new GenericDeadlineMessage(TEST_DEADLINE_NAME, new MessageType("none"), null);

        // when
        String serialized = DeadlineDetails.serialized(TEST_DEADLINE_NAME, descriptor, message, converter);
        DeadlineDetails result = converter.fromStored(serialized, DeadlineDetails.class);

        // then
        assertThat(result.getPayloadClass()).isEqualTo(StoredDeadlineConverter.EMPTY_TYPE);
        assertThat(result.asDeadLineMessage(converter).payload()).isNull();
    }
}
