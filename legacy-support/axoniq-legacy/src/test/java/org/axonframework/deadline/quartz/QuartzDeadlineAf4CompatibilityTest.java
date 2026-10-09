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
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;
import org.quartz.JobDataMap;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that the {@link QuartzDeadlineManager} and Axon Framework 4.13.2's own Quartz deadline manager
 * read each other's job data, for each serializer an Axon Framework 4 application could have used.
 *
 * @author Jakob Hatzl
 */
class QuartzDeadlineAf4CompatibilityTest {

    private static final String BINDER = "org.axonframework.deadline.quartz.DeadlineJob$DeadlineJobDataBinder";
    private static final String DEADLINE_NAME = "paymentDue";
    private static final CompatPayload PAYLOAD = new CompatPayload("text", 3, Instant.parse("2026-10-06T10:15:30Z"));
    private static final UUID METADATA_ID = UUID.fromString("0b9e4c4e-3c0c-4bd4-9a51-5bb1a4d1f2a7");
    private static final Map<String, Object> AF4_METADATA = Map.of(
            "text", "value", "count", 3, "flag", true, "id", METADATA_ID,
            "nested", new LinkedHashMap<>(Map.of("key", "value"))
    );

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
    void aJobOfAxonFramework4IsReadByAxonFramework5(Flavor flavor, CompatScope scope) {
        // given
        Object serializer = flavor.axonFramework4Serializer(axonFramework4);
        Object af4Message = axonFramework4.deadlineMessage(DEADLINE_NAME, PAYLOAD, AF4_METADATA);
        Object af4Scope = scope.axonFramework4(axonFramework4);
        JobDataMap jobData = (JobDataMap) axonFramework4.callStatic(BINDER, "toJobData",
                                                                    serializer, af4Message, af4Scope);
        StoredDeadlineConverter converter = new StoredDeadlineConverter(flavor.axonFramework5Converter());

        // when
        DeadlineMessage result = DeadlineJob.DeadlineJobDataBinder.deadlineMessage(converter, jobData);
        ScopeDescriptor resultScope = DeadlineJob.DeadlineJobDataBinder.deadlineScope(converter, jobData);

        // then
        assertThat(result.getDeadlineName()).isEqualTo(DEADLINE_NAME);
        assertThat(result.identifier()).isEqualTo(axonFramework4.call(af4Message, "getIdentifier"));
        assertThat(result.timestamp()).isEqualTo(axonFramework4.call(af4Message, "getTimestamp"));
        assertThat(result.payload()).isEqualTo(PAYLOAD);
        assertThat(result.metadata()).containsExactlyInAnyOrderEntriesOf(
                Map.of("text", "value", "count", "3", "flag", "true", "id", METADATA_ID.toString(),
                       "nested", flavor.nestedMetadataValue())
        );
        assertThat(resultScope).isEqualTo(scope.axonFramework5());
    }

    @ParameterizedTest
    @MethodSource("org.axonframework.deadline.AxonFramework4#flavorsAndRawPayloads")
    void aJobOfAxonFramework4WithAStringOrBytesPayloadIsReadByAxonFramework5(Flavor flavor, Object payload) {
        // given
        JobDataMap jobData = (JobDataMap) axonFramework4.callStatic(
                BINDER, "toJobData", flavor.axonFramework4Serializer(axonFramework4),
                axonFramework4.deadlineMessage(DEADLINE_NAME, payload, Map.of()),
                CompatScope.SAGA.axonFramework4(axonFramework4)
        );

        // when
        DeadlineMessage result = DeadlineJob.DeadlineJobDataBinder.deadlineMessage(
                new StoredDeadlineConverter(flavor.axonFramework5Converter()), jobData
        );

        // then
        assertThat(result.payload()).isEqualTo(payload);
    }

    /**
     * Axon Framework 4 stores an aliased payload class under its alias, which Axon Framework 5 does not resolve.
     */
    @Test
    void aJobOfAxonFramework4WithAnAliasedPayloadIsReadWithAnUnknownPayload() {
        // given
        XStream xStream = new XStream();
        xStream.alias("compatPayload", CompatPayload.class);
        JobDataMap jobData = (JobDataMap) axonFramework4.callStatic(
                BINDER, "toJobData", axonFramework4.xStreamSerializer(xStream),
                axonFramework4.deadlineMessage(DEADLINE_NAME, PAYLOAD, Map.of()),
                CompatScope.SAGA.axonFramework4(axonFramework4)
        );

        // when
        DeadlineMessage result = DeadlineJob.DeadlineJobDataBinder.deadlineMessage(
                new StoredDeadlineConverter(Flavor.XSTREAM.axonFramework5Converter()), jobData
        );

        // then
        assertThat(result.payload()).isInstanceOfSatisfying(
                UnknownDeadlinePayload.class,
                unknown -> assertThat(unknown.typeName()).isEqualTo("compatPayload")
        );
    }

    @ParameterizedTest
    @EnumSource(Flavor.class)
    void aJobOfAxonFramework4WithoutPayloadIsReadWithoutPayload(Flavor flavor) {
        // given
        Object serializer = flavor.axonFramework4Serializer(axonFramework4);
        Object af4Message = axonFramework4.deadlineMessage(DEADLINE_NAME, null, Map.of());
        JobDataMap jobData = (JobDataMap) axonFramework4.callStatic(
                BINDER, "toJobData", serializer, af4Message, CompatScope.SAGA.axonFramework4(axonFramework4)
        );

        // when
        DeadlineMessage result = DeadlineJob.DeadlineJobDataBinder.deadlineMessage(
                new StoredDeadlineConverter(flavor.axonFramework5Converter()), jobData
        );

        // then
        assertThat(result.payload()).isNull();
    }

    /**
     * Quartz cancels by scope through {@code equals} on the stored scope, against the given scope converted to its
     * stored form and back. Both sides have to arrive at the same value, also when the identifier is no string.
     */
    @ParameterizedTest
    @EnumSource(Flavor.class)
    void aScopeStoredByAxonFramework4MatchesTheScopeAxonFramework5CancelsWith(Flavor flavor) {
        // given
        UUID identifier = UUID.randomUUID();
        Object serializer = flavor.axonFramework4Serializer(axonFramework4);
        JobDataMap jobData = (JobDataMap) axonFramework4.callStatic(
                BINDER, "toJobData", serializer,
                axonFramework4.deadlineMessage(DEADLINE_NAME, PAYLOAD, Map.of()),
                axonFramework4.aggregateScope("MyAggregate", identifier)
        );
        Converter converter = flavor.axonFramework5Converter();
        AggregateScopeDescriptor givenScope = new AggregateScopeDescriptor("MyAggregate", identifier);

        // when
        ScopeDescriptor storedScope = DeadlineJob.DeadlineJobDataBinder.deadlineScope(
                new StoredDeadlineConverter(converter), jobData
        );
        Object roundTripped = converter.convert(converter.convert(givenScope, String.class), givenScope.getClass());

        // then
        assertThat(storedScope).isEqualTo(roundTripped);
    }

    @ParameterizedTest
    @MethodSource("org.axonframework.deadline.AxonFramework4#flavorsAndScopes")
    void aJobOfAxonFramework5IsReadByAxonFramework4(Flavor flavor, CompatScope scope) {
        // given
        DeadlineMessage message = new GenericDeadlineMessage(
                DEADLINE_NAME,
                new GenericMessage(new MessageType(CompatPayload.class), PAYLOAD, Map.of("count", "3")),
                Instant::now
        );
        JobDataMap jobData = DeadlineJob.DeadlineJobDataBinder.toJobData(
                new StoredDeadlineConverter(flavor.axonFramework5Converter()),
                message,
                scope.axonFramework5()
        );
        Object serializer = flavor.axonFramework4Serializer(axonFramework4);

        // when
        Object af4Message = axonFramework4.callStatic(BINDER, "deadlineMessage", serializer, jobData);
        Object af4Scope = axonFramework4.callStatic(BINDER, "deadlineScope", serializer, jobData);

        // then
        assertThat(axonFramework4.call(af4Message, "getDeadlineName")).isEqualTo(DEADLINE_NAME);
        assertThat(axonFramework4.call(af4Message, "getIdentifier")).isEqualTo(message.identifier());
        assertThat(axonFramework4.call(af4Message, "getPayload")).isEqualTo(PAYLOAD);
        assertThat(((Map<?, ?>) axonFramework4.call(af4Message, "getMetaData")).get("count")).isEqualTo("3");
        scope.assertAxonFramework4Scope(axonFramework4, af4Scope);
    }

    @ParameterizedTest
    @MethodSource("org.axonframework.deadline.AxonFramework4#flavorsAndRawPayloads")
    void aJobOfAxonFramework5WithAStringOrBytesPayloadIsReadByAxonFramework4(Flavor flavor, Object payload) {
        // given
        DeadlineMessage message = new GenericDeadlineMessage(
                DEADLINE_NAME, new GenericMessage(new MessageType(payload.getClass()), payload), Instant::now
        );
        JobDataMap jobData = DeadlineJob.DeadlineJobDataBinder.toJobData(
                new StoredDeadlineConverter(flavor.axonFramework5Converter()),
                message,
                CompatScope.SAGA.axonFramework5()
        );

        // when
        Object af4Message = axonFramework4.callStatic(
                BINDER, "deadlineMessage", flavor.axonFramework4Serializer(axonFramework4), jobData
        );

        // then
        assertThat(axonFramework4.call(af4Message, "getPayload")).isEqualTo(payload);
    }

    @ParameterizedTest
    @EnumSource(Flavor.class)
    void aJobOfAxonFramework5WithoutPayloadIsReadByAxonFramework4WithoutPayload(Flavor flavor) {
        // given
        DeadlineMessage message = new GenericDeadlineMessage(DEADLINE_NAME, new MessageType("none"), null);
        JobDataMap jobData = DeadlineJob.DeadlineJobDataBinder.toJobData(
                new StoredDeadlineConverter(flavor.axonFramework5Converter()),
                message,
                CompatScope.SAGA.axonFramework5()
        );

        // when
        Object af4Message = axonFramework4.callStatic(
                BINDER, "deadlineMessage", flavor.axonFramework4Serializer(axonFramework4), jobData
        );

        // then
        assertThat(axonFramework4.call(af4Message, "getPayload")).isNull();
    }
}
