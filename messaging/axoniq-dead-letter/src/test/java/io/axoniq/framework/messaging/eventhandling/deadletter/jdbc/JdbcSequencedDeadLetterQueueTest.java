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
import org.axonframework.common.IdentifierFactory;
import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.common.jdbc.JdbcException;
import org.axonframework.conversion.CachingSupplier;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.GenericDeadLetter;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueueTest;
import io.axoniq.framework.messaging.deadletter.WrongDeadLetterTypeException;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;

import static org.axonframework.common.DateTimeUtils.formatInstant;
import static org.axonframework.common.DateTimeUtils.parseInstant;
import static org.axonframework.common.FutureUtils.joinAndUnwrap;
import static org.axonframework.common.jdbc.JdbcUtils.closeQuietly;
import static org.axonframework.common.jdbc.JdbcUtils.executeUpdates;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Implementation of the {@link SequencedDeadLetterQueueTest}, validating the {@link JdbcSequencedDeadLetterQueue}.
 *
 * @author Steven van Beelen
 */
@Tag("flaky")
class JdbcSequencedDeadLetterQueueTest extends SequencedDeadLetterQueueTest<EventMessage> {

    private static final int MAX_SEQUENCES_AND_SEQUENCE_SIZE = 64;
    private static final String TEST_PROCESSING_GROUP = "some-processing-group";

    private DataSource dataSource;
    private TransactionalExecutorProvider<Connection> executorProvider;
    private JdbcSequencedDeadLetterQueue<EventMessage> jdbcDeadLetterQueue;
    private final JacksonConverter jacksonConverter = new JacksonConverter();
    private final DelegatingEventConverter eventConverter = new DelegatingEventConverter(jacksonConverter);
    private final AtomicLong sequenceCounter = new AtomicLong(0);

    private final DeadLetterSchema schema = DeadLetterSchema.defaultSchema();

    @Override
    protected SequencedDeadLetterQueue<EventMessage> buildTestSubject() {
        dataSource = dataSource();
        executorProvider = new JdbcTransactionalExecutorProvider(dataSource);

        jdbcDeadLetterQueue = JdbcSequencedDeadLetterQueue.builder()
                                                          .processingGroup(TEST_PROCESSING_GROUP)
                                                          .maxSequences(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                                                          .maxSequenceSize(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                                                          .transactionalExecutorProvider(executorProvider)
                                                          .schema(schema)
                                                          .eventConverter(eventConverter)
                                                          .genericConverter(jacksonConverter)
                                                          .build();
        return jdbcDeadLetterQueue;
    }

    private static JDBCDataSource dataSource() {
        JDBCDataSource dataSource = new JDBCDataSource();
        dataSource.setUrl("jdbc:hsqldb:mem:" + JdbcSequencedDeadLetterQueueTest.class.getSimpleName());
        dataSource.setUser("sa");
        dataSource.setPassword("");
        return dataSource;
    }

    @SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
    @BeforeEach
    void setUpJdbc() {
        dropDeadLetterTable(dataSource);
        joinAndUnwrap(jdbcDeadLetterQueue.createSchema(new GenericDeadLetterTableFactory(), null));
    }

    private void dropDeadLetterTable(DataSource dataSource) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            executeUpdates(
                    connection,
                    e -> {
                        throw new JdbcException("Unable to prepare dead letter table", e);
                    },
                    c -> c.prepareStatement("DROP TABLE IF EXISTS " + schema.deadLetterTable())
            );
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to retrieve a Connection to prepare the dead letter table", e);
        } finally {
            closeQuietly(connection);
        }
    }

    @Override
    protected long maxSequences() {
        return MAX_SEQUENCES_AND_SEQUENCE_SIZE;
    }

    @Override
    protected long maxSequenceSize() {
        return MAX_SEQUENCES_AND_SEQUENCE_SIZE;
    }

    @Override
    public DeadLetter<EventMessage> generateInitialLetter() {
        return new GenericDeadLetter<>("sequenceIdentifier", generateEvent(), generateThrowable(),
                                      buildTestContext());
    }

    @Override
    protected DeadLetter<EventMessage> generateFollowUpLetter() {
        return new GenericDeadLetter<>("sequenceIdentifier", generateEvent(), (Throwable) null,
                                      buildTestContext());
    }

    private Context buildTestContext() {
        long seqNo = sequenceCounter.getAndIncrement();
        return Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(seqNo))
                      .withResource(LegacyResources.AGGREGATE_TYPE_KEY, "TestAggregate")
                      .withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "aggregate-" + seqNo)
                      .withResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY, seqNo);
    }

    @Override
    protected DeadLetter<EventMessage> mapToQueueImplementation(DeadLetter<EventMessage> deadLetter) {
        if (deadLetter instanceof JdbcDeadLetter) {
            return deadLetter;
        }
        if (deadLetter instanceof GenericDeadLetter) {
            return new JdbcDeadLetter<>(
                    IdentifierFactory.getInstance().generateIdentifier(),
                    0L,
                    ((GenericDeadLetter<EventMessage>) deadLetter).getSequenceIdentifier().toString(),
                    deadLetter.enqueuedAt(),
                    deadLetter.lastTouched(),
                    deadLetter.cause().orElse(null),
                    deadLetter.diagnostics(),
                    deadLetter.message(),
                    null
            );
        }
        throw new IllegalArgumentException("Can not map dead letter of type " + deadLetter.getClass().getName());
    }

    @Override
    protected void setClock(Clock clock) {
        GenericDeadLetter.clock = clock;
    }

    @Override
    protected ProcessingContext toProcessingContext(Context context) {
        ProcessingContext pc = super.toProcessingContext(context);
        if (pc != null) {
            pc.putResource(JdbcTransactionalExecutorProvider.SUPPLIER_KEY,
                           CachingSupplier.of(() -> new ConnectionExecutor(dataSource::getConnection)));
        }
        return pc;
    }

    @Override
    protected void assertLetter(DeadLetter<? extends EventMessage> expected,
                                DeadLetter<? extends EventMessage> actual) {
        assertMessage(expected.message(), actual.message());
        assertThat(actual.cause()).isEqualTo(expected.cause());
        assertThat(actual.enqueuedAt()).isEqualTo(formatExpected(expected.enqueuedAt()));
        assertThat(actual.lastTouched()).isEqualTo(formatExpected(expected.lastTouched()));
        assertThat(actual.diagnostics()).isEqualTo(expected.diagnostics());
        assertContext(expected.context(), actual.context());
    }

    @Override
    protected void assertMessage(EventMessage expected, EventMessage actual) {
        assertThat(actual.identifier()).isEqualTo(expected.identifier());
        assertThat(actual.type()).isEqualTo(expected.type());
        assertThat(actual.metadata()).isEqualTo(expected.metadata());

        // Payload is stored as raw bytes; deserialize to compare with the original
        Object deserializedPayload = eventConverter.convertPayload(actual, expected.payloadType());
        assertThat(deserializedPayload).isEqualTo(expected.payload());
    }

    /**
     * Format the expected {@link java.time.Instant} to align with the precision as dictated by the
     * {@link org.axonframework.common.DateTimeUtils}. Required as the actual {@code Instants} underwent formatting by
     * the {@link JdbcSequencedDeadLetterQueue} as well, whereas the {@code expected} value did not.
     */
    private static java.time.Instant formatExpected(java.time.Instant expected) {
        return parseInstant(formatInstant(expected));
    }

    @Test
    void invokingEvictWithNonJdbcDeadLetterThrowsWrongDeadLetterTypeException() {
        DeadLetter<EventMessage> testLetter = generateInitialLetter();
        assertThatThrownBy(() -> joinAndUnwrap(jdbcDeadLetterQueue.evict(testLetter, null))).isInstanceOf(WrongDeadLetterTypeException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void buildWithNullProcessingGroupThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.processingGroup(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithEmptyProcessingGroupThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.processingGroup("")).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void buildWithNullTransactionalExecutorProviderThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.transactionalExecutorProvider(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void buildWithNullSchemaThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.schema(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void buildWithNullStatementFactoryThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.statementFactory(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void buildWithNullConverterThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.converter(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void buildWithNullEventConverterThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.eventConverter(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void buildWithNullGenericConverterThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.genericConverter(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithZeroMaxSequencesThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.maxSequences(0)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithNegativeMaxSequencesThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.maxSequences(-1)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithZeroMaxSequenceSizeThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.maxSequenceSize(0)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithNegativeMaxSequenceSizeThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.maxSequenceSize(-1)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithNullClaimDurationThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.claimDuration(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithZeroPageSizeThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.pageSize(0)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithNegativePageSizeThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder = JdbcSequencedDeadLetterQueue.builder();
        assertThatThrownBy(() -> testBuilder.pageSize(-1)).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    void buildWithoutProcessingGroupThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder =
                JdbcSequencedDeadLetterQueue.builder()
                                            .transactionalExecutorProvider(executorProvider)
                                            .statementFactory(mock(DeadLetterStatementFactory.class))
                                            .converter(mock(DeadLetterJdbcConverter.class));

        assertThatThrownBy(testBuilder::build).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    void buildWithoutTransactionalExecutorProviderThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder =
                JdbcSequencedDeadLetterQueue.builder()
                                            .processingGroup(TEST_PROCESSING_GROUP)
                                            .statementFactory(mock(DeadLetterStatementFactory.class))
                                            .converter(mock(DeadLetterJdbcConverter.class));

        assertThatThrownBy(testBuilder::build).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    void buildWithoutStatementFactoryAndGenericConverterThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder =
                JdbcSequencedDeadLetterQueue.builder()
                                            .processingGroup(TEST_PROCESSING_GROUP)
                                            .transactionalExecutorProvider(executorProvider)
                                            .converter(mock(DeadLetterJdbcConverter.class))
                                            .eventConverter(eventConverter);

        assertThatThrownBy(testBuilder::build).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    void buildWithoutStatementFactoryAndEventConverterThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder =
                JdbcSequencedDeadLetterQueue.builder()
                                            .processingGroup(TEST_PROCESSING_GROUP)
                                            .transactionalExecutorProvider(executorProvider)
                                            .converter(mock(DeadLetterJdbcConverter.class))
                                            .genericConverter(jacksonConverter);

        assertThatThrownBy(testBuilder::build).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    void buildWithoutConverterAndGenericConverterThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder =
                JdbcSequencedDeadLetterQueue.builder()
                                            .processingGroup(TEST_PROCESSING_GROUP)
                                            .transactionalExecutorProvider(executorProvider)
                                            .statementFactory(mock(DeadLetterStatementFactory.class))
                                            .eventConverter(eventConverter);

        assertThatThrownBy(testBuilder::build).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    void buildWithoutConverterAndEventConverterThrowsAxonConfigurationException() {
        JdbcSequencedDeadLetterQueue.Builder<EventMessage> testBuilder =
                JdbcSequencedDeadLetterQueue.builder()
                                            .processingGroup(TEST_PROCESSING_GROUP)
                                            .transactionalExecutorProvider(executorProvider)
                                            .statementFactory(mock(DeadLetterStatementFactory.class))
                                            .genericConverter(jacksonConverter);

        assertThatThrownBy(testBuilder::build).isInstanceOf(AxonConfigurationException.class);
    }
}
