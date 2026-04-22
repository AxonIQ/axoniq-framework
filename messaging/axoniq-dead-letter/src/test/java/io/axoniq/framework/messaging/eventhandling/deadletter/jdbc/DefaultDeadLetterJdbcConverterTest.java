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

package io.axoniq.framework.messaging.eventhandling.deadletter.jdbc;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.DateTimeUtils;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import io.axoniq.framework.messaging.deadletter.Cause;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link DefaultDeadLetterJdbcConverter}.
 *
 * @author Steven van Beelen
 */
class DefaultDeadLetterJdbcConverterTest {

    private static final String AGGREGATE_ID = "aggregateId";
    private static final String AGGREGATE_TYPE = "aggregateType";
    private static final long SEQ_NO = 42;
    private static final TrackingToken TRACKING_TOKEN = new GlobalSequenceTrackingToken(1);
    private static final String CAUSE_TYPE = "causeType";
    private static final String CAUSE_MESSAGE = "causeMessage";

    private DeadLetterSchema schema;
    private Converter genericConverter;
    private EventConverter eventConverter;

    private DefaultDeadLetterJdbcConverter<?> testSubject;

    @BeforeEach
    void setUp() {
        schema = DeadLetterSchema.defaultSchema();
        genericConverter = new JacksonConverter();
        eventConverter = new DelegatingEventConverter(genericConverter);

        testSubject = DefaultDeadLetterJdbcConverter.builder()
                                                    .schema(schema)
                                                    .genericConverter(genericConverter)
                                                    .eventConverter(eventConverter)
                                                    .build();
    }

    @Nested
    class ConvertToLetter {

        @Test
        void constructsGenericEventMessageWithTrackingTokenAndAggregateInfoInContext() throws SQLException {
            ResultSet resultSet = mockedResultSet(true, true, true);

            JdbcDeadLetter<?> result = testSubject.convertToLetter(resultSet);

            EventMessage resultMessage = result.message();
            assertThat(resultMessage).isInstanceOf(GenericEventMessage.class);

            Context context = result.context();
            assertThat(context).isNotNull();
            TrackingToken restoredToken = context.getResource(TrackingToken.RESOURCE_KEY);
            assertThat(restoredToken).isEqualTo(TRACKING_TOKEN);
            assertThat(context.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)).isEqualTo(AGGREGATE_ID);
            assertThat(context.getResource(LegacyResources.AGGREGATE_TYPE_KEY)).isEqualTo(AGGREGATE_TYPE);
            assertThat(context.getResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY)).isEqualTo(SEQ_NO);
        }

        @Test
        void constructsGenericEventMessageWithTrackingTokenOnlyInContext() throws SQLException {
            ResultSet resultSet = mockedResultSet(true, false, true);

            JdbcDeadLetter<?> result = testSubject.convertToLetter(resultSet);

            assertThat(result.message()).isInstanceOf(GenericEventMessage.class);

            Context context = result.context();
            assertThat(context).isNotNull();
            TrackingToken restoredToken = context.getResource(TrackingToken.RESOURCE_KEY);
            assertThat(restoredToken).isEqualTo(TRACKING_TOKEN);
            assertThat(context.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)).isNull();
        }

        @Test
        void constructsGenericEventMessageWithAggregateInfoOnlyInContext() throws SQLException {
            ResultSet resultSet = mockedResultSet(false, true, true);

            JdbcDeadLetter<?> result = testSubject.convertToLetter(resultSet);

            assertThat(result.message()).isInstanceOf(GenericEventMessage.class);

            Context context = result.context();
            assertThat(context).isNotNull();
            assertThat(context.getResource(TrackingToken.RESOURCE_KEY)).isNull();
            assertThat(context.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)).isEqualTo(AGGREGATE_ID);
            assertThat(context.getResource(LegacyResources.AGGREGATE_TYPE_KEY)).isEqualTo(AGGREGATE_TYPE);
            assertThat(context.getResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY)).isEqualTo(SEQ_NO);
        }

        @Test
        void constructsGenericEventMessageWithEmptyContext() throws SQLException {
            ResultSet resultSet = mockedResultSet(false, false, true);

            JdbcDeadLetter<?> result = testSubject.convertToLetter(resultSet);

            assertThat(result.message()).isInstanceOf(GenericEventMessage.class);
        }

        @Test
        void constructsGenericEventMessageWithConverter() throws SQLException {
            ResultSet resultSet = mockedResultSet(false, false, true);

            JdbcDeadLetter<?> result = testSubject.convertToLetter(resultSet);

            assertThat(result.message()).isInstanceOf(GenericEventMessage.class);
            assertThat(result.message().payloadType()).isEqualTo(byte[].class);
            assertThat(result.message().payloadAs(String.class)).isEqualTo("some-payload");
        }

        @Test
        void constructsWithCause() throws SQLException {
            ResultSet resultSet = mockedResultSet(true, true, true);

            JdbcDeadLetter<?> result = testSubject.convertToLetter(resultSet);

            Optional<Cause> optionalCause = result.cause();
            assertThat(optionalCause).isPresent();
            Cause resultCause = optionalCause.orElseThrow();
            assertThat(resultCause.type()).isEqualTo(CAUSE_TYPE);
            assertThat(resultCause.message()).isEqualTo(CAUSE_MESSAGE);
        }

        @Test
        void constructsWithoutCause() throws SQLException {
            ResultSet resultSet = mockedResultSet(true, true, false);

            JdbcDeadLetter<?> result = testSubject.convertToLetter(resultSet);

            assertThat(result.cause()).isEmpty();
        }
    }

    private ResultSet mockedResultSet(boolean withToken, boolean isDomainEvent, boolean withCause) throws SQLException {
        ResultSet mock = mock(ResultSet.class);
        String timestamp = DateTimeUtils.formatInstant(Instant.now());
        byte[] serializedPayload = eventConverter.convert("some-payload", byte[].class);
        byte[] serializedMetadata = eventConverter.convert(Metadata.emptyInstance(), byte[].class);

        // Payload mocking
        when(mock.getBytes(schema.payloadColumn())).thenReturn(serializedPayload);
        // Metadata mocking
        when(mock.getBytes(schema.metadataColumn())).thenReturn(serializedMetadata);
        // Event Message mocking
        when(mock.getString(schema.eventIdentifierColumn())).thenReturn(UUID.randomUUID().toString());
        when(mock.getString(schema.typeColumn()))
                .thenReturn(new MessageType("event").toString());
        when(mock.getString(schema.timestampColumn())).thenReturn(timestamp);
        // Token mocking
        if (withToken) {
            when(mock.getString(schema.tokenTypeColumn())).thenReturn(GlobalSequenceTrackingToken.class.getName());
            when(mock.getBytes(schema.tokenColumn()))
                    .thenReturn(genericConverter.convert(TRACKING_TOKEN, byte[].class));
        } else {
            when(mock.getString(schema.tokenTypeColumn())).thenReturn(null);
        }
        // Domain Event mocking
        if (isDomainEvent) {
            when(mock.getString(schema.aggregateIdentifierColumn())).thenReturn(AGGREGATE_ID);
            when(mock.getString(schema.aggregateTypeColumn())).thenReturn(AGGREGATE_TYPE);
            when(mock.getLong(schema.sequenceNumberColumn())).thenReturn(SEQ_NO);
        } else {
            when(mock.getString(schema.aggregateIdentifierColumn())).thenReturn(null);
        }
        // Dead Letter mocking
        when(mock.getString(schema.deadLetterIdentifierColumn())).thenReturn(UUID.randomUUID().toString());
        when(mock.getLong(schema.sequenceIndexColumn())).thenReturn(1337L);
        when(mock.getString(schema.sequenceIdentifierColumn())).thenReturn(UUID.randomUUID().toString());
        when(mock.getString(schema.enqueuedAtColumn())).thenReturn(timestamp);
        when(mock.getString(schema.lastTouchedColumn())).thenReturn(timestamp);
        when(mock.getBytes(schema.diagnosticsColumn())).thenReturn(serializedMetadata);
        // Cause mocking
        if (withCause) {
            when(mock.getString(schema.causeTypeColumn())).thenReturn(CAUSE_TYPE);
            when(mock.getString(schema.causeMessageColumn())).thenReturn(CAUSE_MESSAGE);
        } else {
            when(mock.getString(schema.causeTypeColumn())).thenReturn(null);
        }

        return mock;
    }

    @Nested
    class BuilderValidation {

        @Test
        void buildWithNullSchemaThrowsAxonConfigurationException() {
            DefaultDeadLetterJdbcConverter.Builder<?> testBuilder = DefaultDeadLetterJdbcConverter.builder();

            assertThatThrownBy(() -> testBuilder.schema(null)).isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void buildWithNullGenericConverterThrowsAxonConfigurationException() {
            DefaultDeadLetterJdbcConverter.Builder<?> testBuilder = DefaultDeadLetterJdbcConverter.builder();

            assertThatThrownBy(() -> testBuilder.genericConverter(null)).isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void buildWithNullEventConverterThrowsAxonConfigurationException() {
            DefaultDeadLetterJdbcConverter.Builder<?> testBuilder = DefaultDeadLetterJdbcConverter.builder();

            assertThatThrownBy(() -> testBuilder.eventConverter(null)).isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void buildWithoutTheGenericConverterThrowsAxonConfigurationException() {
            DefaultDeadLetterJdbcConverter.Builder<?> testBuilder =
                    DefaultDeadLetterJdbcConverter.builder()
                                                  .eventConverter(eventConverter);

            assertThatThrownBy(testBuilder::build).isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void buildWithoutTheEventConverterThrowsAxonConfigurationException() {
            DefaultDeadLetterJdbcConverter.Builder<?> testBuilder =
                    DefaultDeadLetterJdbcConverter.builder()
                                                  .genericConverter(genericConverter);

            assertThatThrownBy(testBuilder::build).isInstanceOf(AxonConfigurationException.class);
        }
    }
}
