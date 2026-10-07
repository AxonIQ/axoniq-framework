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

import com.thoughtworks.xstream.XStream;
import org.axonframework.deadline.AxonFramework4;
import org.axonframework.deadline.AxonFramework4.CompatScope;
import org.axonframework.deadline.AxonFramework4.Flavor;
import org.axonframework.deadline.AxonFramework4.Payloads.CompatPayload;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.deadline.UnknownDeadlinePayload;
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
import java.util.UUID;

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
    private static final UUID METADATA_ID = UUID.fromString("0b9e4c4e-3c0c-4bd4-9a51-5bb1a4d1f2a7");
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
    @MethodSource("org.axonframework.deadline.AxonFramework4#flavorsAndScopes")
    void detailsOfAxonFramework4AreReadByAxonFramework5(Flavor flavor, CompatScope scope) {
        // given
        Object serializer = flavor.axonFramework4Serializer(axonFramework4);
        Map<String, Object> metadata =
                Map.of("text", "value", "count", 3, "id", METADATA_ID,
                       "nested", new LinkedHashMap<>(Map.of("key", "value")));
        String details = (String) axonFramework4.callStatic(
                DETAILS, "serialized", DEADLINE_NAME, scope.axonFramework4(axonFramework4),
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
                Map.of("text", "value", "count", "3", "id", METADATA_ID.toString(), "nested", "{\"key\":\"value\"}")
        );
        assertThat(result.getDeserializedScopeDescriptor(converter)).isEqualTo(scope.axonFramework5());
    }

    @ParameterizedTest
    @MethodSource("org.axonframework.deadline.AxonFramework4#flavorsAndRawPayloads")
    void detailsOfAxonFramework4WithAStringOrBytesPayloadAreReadByAxonFramework5(Flavor flavor, Object payload) {
        // given
        String details = (String) axonFramework4.callStatic(
                DETAILS, "serialized", DEADLINE_NAME, CompatScope.SAGA.axonFramework4(axonFramework4),
                axonFramework4.deadlineMessage(DEADLINE_NAME, payload, Map.of()),
                flavor.axonFramework4Serializer(axonFramework4)
        );
        StoredDeadlineConverter converter = new StoredDeadlineConverter(flavor.axonFramework5Converter());

        // when
        DeadlineMessage result = converter.fromStored(details, DeadlineDetails.class).asDeadLineMessage(converter);

        // then
        assertThat(result.payload()).isEqualTo(payload);
    }

    /**
     * Axon Framework 4 stores an aliased payload class under its alias, which Axon Framework 5 does not resolve.
     */
    @Test
    void detailsOfAxonFramework4WithAnAliasedPayloadAreReadWithAnUnknownPayload() {
        // given
        XStream xStream = new XStream();
        xStream.alias("compatPayload", CompatPayload.class);
        String details = (String) axonFramework4.callStatic(
                DETAILS, "serialized", DEADLINE_NAME, CompatScope.SAGA.axonFramework4(axonFramework4),
                axonFramework4.deadlineMessage(DEADLINE_NAME, PAYLOAD, Map.of()),
                axonFramework4.xStreamSerializer(xStream)
        );
        StoredDeadlineConverter converter = new StoredDeadlineConverter(Flavor.XSTREAM.axonFramework5Converter());

        // when
        DeadlineMessage result = converter.fromStored(details, DeadlineDetails.class).asDeadLineMessage(converter);

        // then
        assertThat(result.payload()).isInstanceOfSatisfying(
                UnknownDeadlinePayload.class,
                unknown -> assertThat(unknown.typeName()).isEqualTo("compatPayload")
        );
    }

    @ParameterizedTest
    @EnumSource(Flavor.class)
    void detailsOfAxonFramework4WithoutPayloadAreReadWithoutPayload(Flavor flavor) {
        // given
        String details = (String) axonFramework4.callStatic(
                DETAILS, "serialized", DEADLINE_NAME, CompatScope.SAGA.axonFramework4(axonFramework4),
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
    @MethodSource("org.axonframework.deadline.AxonFramework4#flavorsAndScopes")
    void detailsOfAxonFramework5AreReadByAxonFramework4(Flavor flavor, CompatScope scope) {
        // given
        DeadlineMessage message = new GenericDeadlineMessage(
                DEADLINE_NAME,
                new GenericMessage(new MessageType(CompatPayload.class), PAYLOAD, Map.of("count", "3")),
                Instant::now
        );
        String details = DeadlineDetails.serialized(DEADLINE_NAME,
                                                    scope.axonFramework5(),
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
        scope.assertAxonFramework4Scope(axonFramework4, af4Scope);
    }

    @ParameterizedTest
    @MethodSource("org.axonframework.deadline.AxonFramework4#flavorsAndRawPayloads")
    void detailsOfAxonFramework5WithAStringOrBytesPayloadAreReadByAxonFramework4(Flavor flavor, Object payload) {
        // given
        DeadlineMessage message = new GenericDeadlineMessage(
                DEADLINE_NAME, new GenericMessage(new MessageType(payload.getClass()), payload), Instant::now
        );
        StoredDeadlineConverter converter = new StoredDeadlineConverter(flavor.axonFramework5Converter());
        String details =
                DeadlineDetails.serialized(DEADLINE_NAME, CompatScope.SAGA.axonFramework5(), message, converter);
        Object serializer = flavor.axonFramework4Serializer(axonFramework4);

        // when
        Object af4Details = axonFramework4.deserialize(serializer, details, DETAILS);
        Object af4Message = axonFramework4.call(af4Details, "asDeadLineMessage", serializer);

        // then
        assertThat(axonFramework4.call(af4Message, "getPayload")).isEqualTo(payload);
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
