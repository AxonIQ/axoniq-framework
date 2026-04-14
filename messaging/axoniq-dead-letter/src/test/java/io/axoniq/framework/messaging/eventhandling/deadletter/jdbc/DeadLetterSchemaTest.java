/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.messaging.eventhandling.deadletter.jdbc;

import org.axonframework.common.AxonConfigurationException;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Test class validating the {@link DeadLetterSchema}.
 *
 * @author Steven van Beelen
 */
class DeadLetterSchemaTest {

    public static final String TEST_COLUMN_NAME = "some-name";

    @Test
    void buildWithDeadLetterTableReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .deadLetterTable(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.deadLetterTable()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullDeadLetterTableThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.deadLetterTable(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyDeadLetterTableThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.deadLetterTable("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithDeadLetterIdentifierColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .deadLetterIdentifierColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.deadLetterIdentifierColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullDeadLetterIdentifierColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.deadLetterIdentifierColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyDeadLetterIdentifierColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.deadLetterIdentifierColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithProcessingGroupColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .processingGroupColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.processingGroupColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullProcessingGroupColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.processingGroupColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyProcessingGroupColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.processingGroupColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithSequenceIdentifierColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .sequenceIdentifierColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.sequenceIdentifierColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullSequenceIdentifierColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.sequenceIdentifierColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptySequenceIdentifierColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.sequenceIdentifierColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithSequenceIndexColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .sequenceIndexColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.sequenceIndexColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullSequenceIndexColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.sequenceIndexColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptySequenceIndexColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.sequenceIndexColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEventTypeColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .eventTypeColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.eventTypeColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullEventTypeColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.eventTypeColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyEventTypeColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.eventTypeColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEventIdentifierColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .eventIdentifierColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.eventIdentifierColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullEventIdentifierColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.eventIdentifierColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyEventIdentifierColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.eventIdentifierColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithNameColumnReturnsConfiguredColumnType() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .typeColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.typeColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullTypeColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.typeColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyTypeColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.typeColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithTimeStampColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .timestampColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.timestampColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullTimeStampColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.timestampColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyTimeStampColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.timestampColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithPayloadColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .payloadColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.payloadColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullPayloadColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.payloadColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyPayloadColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.payloadColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithMetadataColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .metadataColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.metadataColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullMetadataColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.metadataColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyMetadataColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.metadataColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithAggregateTypeColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .aggregateTypeColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.aggregateTypeColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullAggregateTypeColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.aggregateTypeColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyAggregateTypeColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.aggregateTypeColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithAggregateIdentifierColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .aggregateIdentifierColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.aggregateIdentifierColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullAggregateIdentifierColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.aggregateIdentifierColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyAggregateIdentifierColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.aggregateIdentifierColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithSequenceNumberColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .sequenceNumberColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.sequenceNumberColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullSequenceNumberColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.sequenceNumberColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptySequenceNumberColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.sequenceNumberColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithTokenTypeColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .tokenTypeColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.tokenTypeColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullTokenTypeColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.tokenTypeColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyTokenTypeColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.tokenTypeColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithTokenColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .tokenColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.tokenColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullTokenColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.tokenColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyTokenColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.tokenColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEnqueuedAtColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .enqueuedAtColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.enqueuedAtColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullEnqueuedAtColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.enqueuedAtColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyEnqueuedAtColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.enqueuedAtColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithLastTouchedColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .lastTouchedColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.lastTouchedColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullLastTouchedColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.lastTouchedColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyLastTouchedColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.lastTouchedColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithProcessingStartedColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .processingStartedColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.processingStartedColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullProcessingStartedColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.processingStartedColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyProcessingStartedColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.processingStartedColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithCauseTypeColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .causeTypeColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.causeTypeColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullCauseTypeColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.causeTypeColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyCauseTypeColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.causeTypeColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithCauseMessageColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .causeMessageColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.causeMessageColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullCauseMessageColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.causeMessageColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyCauseMessageColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.causeMessageColumn("")).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithDiagnosticsColumnReturnsConfiguredColumnName() {
        DeadLetterSchema result = DeadLetterSchema.builder()
                                                  .diagnosticsColumn(TEST_COLUMN_NAME)
                                                  .build();

        assertThat(result.diagnosticsColumn()).isEqualTo(TEST_COLUMN_NAME);
    }

    @Test
    void buildWithNullDiagnosticsColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.diagnosticsColumn(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyDiagnosticsColumnThrowsAxonConfigurationException() {
        DeadLetterSchema.Builder testBuilder = DeadLetterSchema.builder();

        assertThatThrownBy(() -> testBuilder.diagnosticsColumn("")).isInstanceOf(AxonConfigurationException.class);
    }
}