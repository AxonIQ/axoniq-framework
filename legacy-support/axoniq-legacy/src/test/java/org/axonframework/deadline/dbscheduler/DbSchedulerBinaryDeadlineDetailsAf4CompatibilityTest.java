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

import com.thoughtworks.xstream.XStream;
import org.axonframework.conversion.Converter;
import org.axonframework.deadline.AxonFramework4;
import org.axonframework.deadline.AxonFramework4.CompatScope;
import org.axonframework.deadline.AxonFramework4.Flavor;
import org.axonframework.deadline.AxonFramework4.Payloads.CompatPayload;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.deadline.UnknownDeadlinePayload;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.modelling.command.AggregateScopeDescriptor;
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
 * Test class validating that the {@link DbSchedulerDeadlineManager} and Axon Framework 4.13.2's own db-scheduler
 * deadline manager read each other's {@link DbSchedulerBinaryDeadlineDetails}, stored with Java serialization,
 * db-scheduler's default, for each serializer an Axon Framework 4 application could have used.
 *
 * @author Jakob Hatzl
 */
class DbSchedulerBinaryDeadlineDetailsAf4CompatibilityTest {

    private static final String DETAILS = DbSchedulerBinaryDeadlineDetails.class.getName();
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
        Map<String, Object> metadata =
                Map.of("text", "value", "count", 3, "id", METADATA_ID,
                       "nested", new LinkedHashMap<>(Map.of("key", "value")));
        Object af4Details = axonFramework4.callStatic(
                DETAILS, "serialized", DEADLINE_NAME, scope.axonFramework4(axonFramework4),
                axonFramework4.deadlineMessage(DEADLINE_NAME, PAYLOAD, metadata),
                flavor.axonFramework4Serializer(axonFramework4)
        );
        StoredDeadlineConverter converter = new StoredDeadlineConverter(flavor.axonFramework5Converter());

        // when
        byte[] stored = AxonFramework4.javaSerialize(af4Details);
        DbSchedulerBinaryDeadlineDetails result =
                (DbSchedulerBinaryDeadlineDetails) AxonFramework4.javaDeserializeAsAxonFramework5(stored);
        DeadlineMessage resultMessage = result.asDeadLineMessage(converter);

        // then
        assertThat(result.getD()).isEqualTo(DEADLINE_NAME);
        assertThat(resultMessage.payload()).isEqualTo(PAYLOAD);
        assertThat(resultMessage.metadata()).containsExactlyInAnyOrderEntriesOf(
                Map.of("text", "value", "count", "3", "id", METADATA_ID.toString(),
                       "nested", flavor.nestedMetadataValue())
        );
        assertThat(result.getDeserializedScopeDescriptor(converter)).isEqualTo(scope.axonFramework5());
    }

    @ParameterizedTest
    @MethodSource("org.axonframework.deadline.AxonFramework4#flavorsAndRawPayloads")
    void detailsOfAxonFramework4WithAStringOrBytesPayloadAreReadByAxonFramework5(Flavor flavor, Object payload) {
        // given
        Object af4Details = axonFramework4.callStatic(
                DETAILS, "serialized", DEADLINE_NAME, CompatScope.SAGA.axonFramework4(axonFramework4),
                axonFramework4.deadlineMessage(DEADLINE_NAME, payload, Map.of()),
                flavor.axonFramework4Serializer(axonFramework4)
        );
        StoredDeadlineConverter converter = new StoredDeadlineConverter(flavor.axonFramework5Converter());

        // when
        DbSchedulerBinaryDeadlineDetails details = (DbSchedulerBinaryDeadlineDetails)
                AxonFramework4.javaDeserializeAsAxonFramework5(AxonFramework4.javaSerialize(af4Details));
        DeadlineMessage result = details.asDeadLineMessage(converter);

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
        Object af4Details = axonFramework4.callStatic(
                DETAILS, "serialized", DEADLINE_NAME, CompatScope.SAGA.axonFramework4(axonFramework4),
                axonFramework4.deadlineMessage(DEADLINE_NAME, PAYLOAD, Map.of()),
                axonFramework4.xStreamSerializer(xStream)
        );
        StoredDeadlineConverter converter = new StoredDeadlineConverter(Flavor.XSTREAM.axonFramework5Converter());

        // when
        DbSchedulerBinaryDeadlineDetails details = (DbSchedulerBinaryDeadlineDetails)
                AxonFramework4.javaDeserializeAsAxonFramework5(AxonFramework4.javaSerialize(af4Details));
        DeadlineMessage result = details.asDeadLineMessage(converter);

        // then
        assertThat(result.payload()).isInstanceOfSatisfying(
                UnknownDeadlinePayload.class,
                unknown -> assertThat(unknown.typeName()).isEqualTo("compatPayload")
        );
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
        DbSchedulerBinaryDeadlineDetails details = DbSchedulerBinaryDeadlineDetails.serialized(
                DEADLINE_NAME,
                scope.axonFramework5(),
                message,
                new StoredDeadlineConverter(flavor.axonFramework5Converter())
        );
        Object serializer = flavor.axonFramework4Serializer(axonFramework4);

        // when
        Object af4Details = axonFramework4.javaDeserializeAsAxonFramework4(AxonFramework4.javaSerialize(details));
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
        DbSchedulerBinaryDeadlineDetails details =
                DbSchedulerBinaryDeadlineDetails.serialized(DEADLINE_NAME, CompatScope.SAGA.axonFramework5(),
                                                            message, converter);
        Object serializer = flavor.axonFramework4Serializer(axonFramework4);

        // when
        Object af4Message = axonFramework4.call(
                axonFramework4.javaDeserializeAsAxonFramework4(AxonFramework4.javaSerialize(details)),
                "asDeadLineMessage", serializer
        );

        // then
        assertThat(axonFramework4.call(af4Message, "getPayload")).isEqualTo(payload);
    }

    @ParameterizedTest
    @EnumSource(Flavor.class)
    void detailsOfAxonFramework5WithoutPayloadAreReadByAxonFramework4WithoutPayload(Flavor flavor) {
        // given
        DbSchedulerBinaryDeadlineDetails details = DbSchedulerBinaryDeadlineDetails.serialized(
                DEADLINE_NAME,
                new SagaScopeDescriptor("MySaga", "sagaId"),
                new GenericDeadlineMessage(DEADLINE_NAME, new MessageType("none"), null),
                new StoredDeadlineConverter(flavor.axonFramework5Converter())
        );
        Object serializer = flavor.axonFramework4Serializer(axonFramework4);

        // when
        Object af4Details = axonFramework4.javaDeserializeAsAxonFramework4(AxonFramework4.javaSerialize(details));

        // then
        assertThat(axonFramework4.call(axonFramework4.call(af4Details, "asDeadLineMessage", serializer),
                                       "getPayload")).isNull();
    }

    /**
     * db-scheduler cancels by scope by comparing the stored form of the given scope with the stored ones, so both
     * versions have to store a scope in exactly the same form.
     */
    @ParameterizedTest
    @EnumSource(Flavor.class)
    void bothVersionsStoreAScopeInTheSameForm(Flavor flavor) {
        // given
        UUID identifier = UUID.randomUUID();
        Object serializer = flavor.axonFramework4Serializer(axonFramework4);
        Converter converter = flavor.axonFramework5Converter();
        Map<ScopeDescriptor, Object> scopes = Map.of(
                new SagaScopeDescriptor("MySaga", "sagaId"), CompatScope.SAGA.axonFramework4(axonFramework4),
                new AggregateScopeDescriptor("MyAggregate", identifier),
                axonFramework4.aggregateScope("MyAggregate", identifier)
        );

        // when / then
        scopes.forEach((af5Scope, af4Scope) -> assertThat(converter.convert(af5Scope, byte[].class))
                .isEqualTo(axonFramework4.serialize(serializer, af4Scope, byte[].class)));
    }
}
