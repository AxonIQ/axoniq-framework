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

import org.axonframework.deadline.AxonFramework4;
import org.axonframework.deadline.AxonFramework4.Flavor;
import org.axonframework.deadline.AxonFramework4.Payloads.CompatPayload;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.modelling.saga.SagaScopeDescriptor;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that the {@link JobRunrDeadlineManager} and Axon Framework 4.13.2's own JobRunr deadline
 * manager read each other's {@link DeadlineDetails}, for each serializer an Axon Framework 4 application could have
 * used.
 *
 * @author Jakob Hatzl
 */
class JobRunrDeadlineAf4CompatibilityTest {

    private static final String DETAILS = "org.axonframework.deadline.jobrunr.DeadlineDetails";
    private static final String DEADLINE_NAME = "paymentDue";
    private static final CompatPayload PAYLOAD = new CompatPayload("text", 3, Instant.parse("2026-10-06T10:15:30Z"));

    private static AxonFramework4 axonFramework4;

    @BeforeAll
    static void openAxonFramework4() throws IOException {
        axonFramework4 = new AxonFramework4();
    }

    @AfterAll
    static void closeAxonFramework4() throws IOException {
        axonFramework4.close();
    }

    @ParameterizedTest
    @EnumSource(Flavor.class)
    void detailsOfAxonFramework4AreReadByAxonFramework5(Flavor flavor) {
        // given
        Object serializer = flavor.axonFramework4Serializer(axonFramework4);
        Map<String, Object> metadata =
                Map.of("text", "value", "count", 3, "nested", new LinkedHashMap<>(Map.of("key", "value")));
        String details = (String) axonFramework4.callStatic(
                DETAILS, "serialized", DEADLINE_NAME, axonFramework4.sagaScope("MySaga", "sagaId"),
                axonFramework4.deadlineMessage(DEADLINE_NAME, PAYLOAD, metadata), serializer
        );
        StoredDeadlineConverter converter = new StoredDeadlineConverter(flavor.axonFramework5Converter());

        // when
        DeadlineDetails result = converter.fromStored(details, DeadlineDetails.class);
        DeadlineMessage resultMessage = result.asDeadLineMessage(converter);

        // then
        assertThat(result.getDeadlineName()).isEqualTo(DEADLINE_NAME);
        assertThat(resultMessage.payload()).isEqualTo(PAYLOAD);
        assertThat(resultMessage.metadata()).containsExactlyInAnyOrderEntriesOf(
                Map.of("text", "value", "count", "3", "nested", "{\"key\":\"value\"}")
        );
        assertThat(result.getDeserializedScopeDescriptor(converter)).isEqualTo(new SagaScopeDescriptor("MySaga",
                                                                                                         "sagaId"));
    }

    @ParameterizedTest
    @EnumSource(Flavor.class)
    void detailsOfAxonFramework4WithoutPayloadAreReadWithoutPayload(Flavor flavor) {
        // given
        String details = (String) axonFramework4.callStatic(
                DETAILS, "serialized", DEADLINE_NAME, axonFramework4.sagaScope("MySaga", "sagaId"),
                axonFramework4.deadlineMessage(DEADLINE_NAME, null, Map.of()),
                flavor.axonFramework4Serializer(axonFramework4)
        );
        StoredDeadlineConverter converter = new StoredDeadlineConverter(flavor.axonFramework5Converter());

        // when
        DeadlineMessage result = converter.fromStored(details, DeadlineDetails.class).asDeadLineMessage(converter);

        // then
        assertThat(result.payload()).isNull();
    }

    @ParameterizedTest
    @EnumSource(Flavor.class)
    void detailsOfAxonFramework5AreReadByAxonFramework4(Flavor flavor) {
        // given
        DeadlineMessage message = new GenericDeadlineMessage(
                DEADLINE_NAME,
                new GenericMessage(new MessageType(CompatPayload.class), PAYLOAD, Map.of("count", "3")),
                Instant::now
        );
        String details = DeadlineDetails.serialized(DEADLINE_NAME,
                                                    new SagaScopeDescriptor("MySaga", "sagaId"),
                                                    message,
                                                    new StoredDeadlineConverter(flavor.axonFramework5Converter()));
        Object serializer = flavor.axonFramework4Serializer(axonFramework4);

        // when
        Object af4Details = axonFramework4.deserialize(serializer, details, DETAILS);
        Object af4Message = axonFramework4.call(af4Details, "asDeadLineMessage", serializer);
        Object af4Scope = axonFramework4.call(af4Details, "getDeserializedScopeDescriptor", serializer);

        // then
        assertThat(axonFramework4.call(af4Message, "getDeadlineName")).isEqualTo(DEADLINE_NAME);
        assertThat(axonFramework4.call(af4Message, "getPayload")).isEqualTo(PAYLOAD);
        assertThat(((Map<?, ?>) axonFramework4.call(af4Message, "getMetaData")).get("count")).isEqualTo("3");
        assertThat(axonFramework4.call(af4Scope, "getType")).isEqualTo("MySaga");
        assertThat(axonFramework4.call(af4Scope, "getIdentifier")).isEqualTo("sagaId");
    }

    @ParameterizedTest
    @EnumSource(Flavor.class)
    void detailsOfAxonFramework5WithoutPayloadAreReadByAxonFramework4WithoutPayload(Flavor flavor) {
        // given
        String details = DeadlineDetails.serialized(
                DEADLINE_NAME,
                new SagaScopeDescriptor("MySaga", "sagaId"),
                new GenericDeadlineMessage(DEADLINE_NAME, new MessageType("none"), null),
                new StoredDeadlineConverter(flavor.axonFramework5Converter())
        );
        Object serializer = flavor.axonFramework4Serializer(axonFramework4);

        // when
        Object af4Details = axonFramework4.deserialize(serializer, details, DETAILS);

        // then
        assertThat(axonFramework4.call(axonFramework4.call(af4Details, "asDeadLineMessage", serializer),
                                       "getPayload")).isNull();
    }
}
