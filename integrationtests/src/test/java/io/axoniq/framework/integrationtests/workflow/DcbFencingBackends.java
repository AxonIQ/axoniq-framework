package io.axoniq.framework.integrationtests.workflow;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngine;
import io.axoniq.framework.postgresql.PostgresqlEventStorageEngine;
import io.axoniq.framework.testcontainer.AxonServerContainer;
import io.axoniq.framework.testcontainer.AxonServerContainerUtils;
import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowEventProcessingRegistrationEnhancer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowExecution;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTagResolver;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.common.jdbc.ConnectionProvider;
import org.axonframework.conversion.CachingSupplier;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.core.unitofwork.transaction.Transaction;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.TAG_WORKFLOW_ID;

/**
 * Backend rig for the DCB append-conditions hunt (scenarios S2/S3/S4): builds the ConcurrentWriterFencingTest node
 * topology over an in-memory, Axon Server (DCB context), or PostgreSQL DCB event store, and captures the runtime's
 * "was rejected: another writer" warnings so tests can assert on fencing per workflow id.
 */
final class DcbFencingBackends {

    static final String MODULE_NAME = WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME;

    enum Backend {IN_MEMORY, AXON_SERVER, POSTGRES}

    private static AxonServerContainer axonServer;
    private static AxonServerConnection axonServerConnection;
    private static PostgreSQLContainer<?> postgres;
    private static final List<HikariDataSource> pgDataSources = new ArrayList<>();
    /** Postgres engines need a per-unit-of-work ConnectionExecutor; remembers each engine's datasource. */
    private static final Map<EventStorageEngine, DataSource> PG_SOURCES =
            java.util.Collections.synchronizedMap(new IdentityHashMap<>());

    private DcbFencingBackends() {
    }

    /** Fresh, empty store for the given backend. Real-backend stores are purged/truncated before being handed out. */
    static EventStorageEngine freshStore(Backend backend) {
        return switch (backend) {
            case IN_MEMORY -> new InMemoryEventStorageEngine();
            case AXON_SERVER -> freshAxonServerStore();
            case POSTGRES -> freshPostgresStore();
        };
    }

    /** Host and gRPC port of the shared Axon Server container, starting it if needed (S5 multi-JVM rig). */
    static synchronized String[] axonServerAddress() {
        if (axonServer == null) {
            freshAxonServerStore();
        }
        return new String[]{axonServer.getHost(), String.valueOf(axonServer.getGrpcPort())};
    }

    private static synchronized EventStorageEngine freshAxonServerStore() {
        if (axonServer == null) {
            axonServer = new AxonServerContainer("docker.axoniq.io/axoniq/axonserver:2025.2.7")
                    .withAxonServerHostname("localhost")
                    .withDevMode(true)
                    .withDcbContext(true);
            axonServer.start();
        } else {
            try {
                AxonServerContainerUtils.purgeEventsFromAxonServer(
                        axonServer.getHost(), axonServer.getHttpPort(), "default", true);
            } catch (Exception e) {
                throw new IllegalStateException("purge failed", e);
            }
        }
        if (axonServerConnection == null || !axonServerConnection.isConnected()) {
            axonServerConnection = AxonServerConnectionFactory
                    .forClient("dcb-hunt-" + System.nanoTime())
                    .routingServers(new ServerAddress(axonServer.getHost(), axonServer.getGrpcPort()))
                    .build()
                    .connect("default");
        }
        return new AxonServerEventStorageEngine(axonServerConnection, converter());
    }

    private static synchronized EventStorageEngine freshPostgresStore() {
        // Diagnostic: show finalization activity ("finalizePositions completed ...") of the Postgres engine.
        Configurator.setLevel("io.axoniq.framework.postgresql", Level.DEBUG);
        if (postgres == null) {
            postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("dcbhunt").withUsername("hunt").withPassword("hunt");
            postgres.start();
        } else {
            try (var connection = DriverManager.getConnection(postgres.getJdbcUrl(),
                                                              postgres.getUsername(),
                                                              postgres.getPassword());
                 var statement = connection.createStatement()) {
                statement.execute("TRUNCATE events, tags, consistency_tags RESTART IDENTITY CASCADE");
            } catch (Exception e) {
                throw new IllegalStateException("truncate failed", e);
            }
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(1);
        config.setAutoCommit(false);
        HikariDataSource dataSource = new HikariDataSource(config);
        pgDataSources.add(dataSource);
        // ponytail: PostgresqlEventStorageEngine.close() is package-private; engines and datasources leak until JVM
        // exit. Fine for a test JVM, revisit if a suite ever runs hundreds of rounds.
        var engine = new PostgresqlEventStorageEngine(dataSource, converter());
        PG_SOURCES.put(engine, dataSource);
        return engine;
    }

    /**
     * Installs a per-unit-of-work JDBC connection on every processing context, the way Spring's transaction manager
     * does in production. Without it, PostgresqlEventStorageEngine#appendEvents fails with "A connection executor
     * must be present in the processing context."
     */
    private static TransactionManager jdbcTransactionManager(DataSource dataSource) {
        return new TransactionManager() {
            @Override
            public Transaction startTransaction() {
                return new Transaction() {
                    @Override
                    public void commit() {
                    }

                    @Override
                    public void rollback() {
                    }
                };
            }

            @Override
            public void attachToProcessingLifecycle(ProcessingLifecycle processingLifecycle) {
                processingLifecycle.runOnPreInvocation(pc -> {
                    var connectionProvider = new LazyConnection(dataSource);
                    pc.putResource(JdbcTransactionalExecutorProvider.SUPPLIER_KEY,
                                   CachingSupplier.of(() -> new ConnectionExecutor(connectionProvider)));
                    pc.onCommit(p -> {
                        try {
                            connectionProvider.commit();
                            return CompletableFuture.completedFuture(null);
                        } catch (SQLException e) {
                            return CompletableFuture.failedFuture(e);
                        } finally {
                            connectionProvider.close();
                        }
                    });
                    pc.onError((p, phase, e) -> {
                        try {
                            connectionProvider.rollback();
                        } catch (SQLException se) {
                            e.addSuppressed(se);
                        } finally {
                            connectionProvider.close();
                        }
                    });
                });
            }
        };
    }

    /** One lazily opened, non-auto-commit connection per unit of work. */
    private static final class LazyConnection implements ConnectionProvider {

        private final DataSource dataSource;
        private Connection connection;

        private LazyConnection(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public synchronized Connection getConnection() throws SQLException {
            if (connection == null) {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
            }
            return connection;
        }

        synchronized void commit() throws SQLException {
            if (connection != null) {
                connection.commit();
            }
        }

        synchronized void rollback() throws SQLException {
            if (connection != null) {
                connection.rollback();
            }
        }

        synchronized void close() {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                // closing failures leave the pool to clean up
            }
        }
    }

    static EventConverter converter() {
        return new DelegatingEventConverter(new JacksonConverter());
    }

    /** Same wiring as ConcurrentWriterFencingTest#startNode, parameterized by store and workflow instance. */
    static Node startNode(EventStorageEngine store, String moduleName, Object workflow) {
        var pgDataSource = PG_SOURCES.get(store);
        var configurer = WorkflowConfigurer.create();
        if (pgDataSource != null) {
            configurer.componentRegistry(cr -> cr.registerComponent(
                    TransactionManager.class, cfg -> jdbcTransactionManager(pgDataSource)));
        }
        configurer.componentRegistry(cr -> cr
                // These connector jars are on the test classpath for the real-backend rigs; their ServiceLoader
                // enhancers must not wire a default Axon Server connection or Postgres engine into the node.
                .disableEnhancer(
                        "io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer")
                .disableEnhancer("io.axoniq.framework.postgresql.PostgresqlConfigurationEnhancer")
                .registerComponent(EventStorageEngine.class, cfg -> store)
                .registerComponent(MutableWorkflowHistoryRepository.class,
                                   cfg -> new InMemoryWorkflowHistoryRepository())
                .registerComponent(TokenStore.class,
                                   "TokenStore[" + MODULE_NAME + "]",
                                   cfg -> new InMemoryTokenStore())
                .registerModule(
                        WorkflowModule.defaults(moduleName, SimpleWorkflowContext.class)
                                      .workflowContextFactory(c -> new SimpleWorkflowContextFactory())
                                      .definition(d -> d.autodetected(c -> workflow))
                ));
        return new Node(configurer.start());
    }

    /** Appends an event straight to the store, bypassing the engine, with the workflow tags resolved. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static void appendDirectly(EventStorageEngine store, EventMessage eventMessage) {
        var tags = new LinkedHashSet<>(new WorkflowEventTagResolver().resolve(eventMessage));
        EventStorageEngine.AppendTransaction transaction =
                store.appendEvents(AppendCondition.none(),
                                   null,
                                   List.of(new GenericTaggedEventMessage<>(eventMessage, Set.copyOf(tags))))
                     .join();
        var commitResult = transaction.commit().join();
        // Drive the after-commit phase too: on Postgres this finalizes the event, without it the
        // event stays invisible to sourcing and streaming for up to 60 seconds.
        transaction.afterCommit(commitResult).join();
    }

    static List<EventMessage> eventsWithTags(EventStorageEngine store, Tag... tags) {
        MessageStream<EventMessage> stream = store.source(
                SourcingCondition.conditionFor(EventCriteria.havingTags(tags))
        );
        try {
            return stream.reduce(new ArrayList<EventMessage>(), (events, entry) -> {
                if (!(entry.message() instanceof TerminalEventMessage)) {
                    events.add(entry.message());
                }
                return events;
            }).join();
        } finally {
            stream.close();
        }
    }

    static List<EventMessage> eventsNamed(EventStorageEngine store, String workflowId, String localName) {
        return eventsWithTags(store, workflowTag(workflowId)).stream()
                .filter(event -> event.type().qualifiedName().localName().equals(localName))
                .toList();
    }

    static List<String> eventNames(EventStorageEngine store, String workflowId) {
        return eventsWithTags(store, workflowTag(workflowId)).stream()
                .map(event -> event.type().qualifiedName().localName())
                .toList();
    }

    static Tag workflowTag(String workflowId) {
        return Tag.of(TAG_WORKFLOW_ID, workflowId);
    }

    static final class Node implements AutoCloseable {

        final AxonConfiguration configuration;
        final WorkflowEngine workflowEngine;

        Node(AxonConfiguration configuration) {
            this.configuration = configuration;
            this.workflowEngine = configuration.getComponent(WorkflowEngine.class);
        }

        void publish(Object event) {
            var resolver = configuration.getComponent(MessageTypeResolver.class);
            var converter = configuration.getComponent(EventConverter.class);
            var sink = configuration.getComponent(EventSink.class);
            var message = new org.axonframework.messaging.eventhandling.GenericEventMessage(
                    resolver.resolveOrThrow(event), event).withConverter(converter);
            // Publish inside a unit of work, the production path. A bare publish(null, ...) skips the
            // after-commit phase, which on Postgres leaves the event unfinalized (invisible) for up to 60 s.
            var factory = configuration.getComponent(
                    org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory.class);
            try {
                factory.create().executeWithResult(ctx -> sink.publish(ctx, message)).join();
            } catch (java.util.concurrent.CompletionException e) {
                // Observed flake: a just-closed node's stale stream callback on the shared store throws
                // RejectedExecutionException into the publisher's unit of work. Retry once (see plan notes).
                if (!(rootCause(e) instanceof java.util.concurrent.RejectedExecutionException)) {
                    throw e;
                }
                factory.create().executeWithResult(ctx -> sink.publish(ctx, message)).join();
            }
        }

        private static Throwable rootCause(Throwable t) {
            while (t.getCause() != null && t.getCause() != t) {
                t = t.getCause();
            }
            return t;
        }

        List<String> runningWorkflowIds() {
            return workflowEngine.workflowExecutions().stream()
                                 .map(WorkflowExecution::workflowId)
                                 .sorted()
                                 .toList();
        }

        @Override
        public void close() {
            workflowEngine.shutdown();
            configuration.shutdown();
        }
    }

    /** Captures the engine's append-rejection WARN logs so tests can count "was rejected" per workflow id. */
    static final class RejectionLog extends AbstractAppender implements AutoCloseable {

        private final List<LogEvent> events = new CopyOnWriteArrayList<>();
        private final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        private final LoggerConfig loggerConfig;

        RejectionLog() {
            super("RejectionLog-" + UUID.randomUUID(), null, null, true, Property.EMPTY_ARRAY);
            String loggerName = SimpleWorkflowExecution.class.getName();
            LoggerConfig nearest = context.getConfiguration().getLoggerConfig(loggerName);
            if (nearest.getName().equals(loggerName)) {
                loggerConfig = nearest;
            } else {
                loggerConfig = new LoggerConfig(loggerName, nearest.getLevel(), true);
                context.getConfiguration().addLogger(loggerName, loggerConfig);
            }
            start();
            loggerConfig.addAppender(this, Level.ALL, null);
            context.updateLoggers();
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }

        List<String> rejectionsFor(String workflowId) {
            return events.stream()
                         .filter(e -> e.getLevel() == Level.WARN)
                         .map(e -> e.getMessage().getFormattedMessage())
                         .filter(m -> m.contains("was rejected")
                                 && m.contains("workflow '" + workflowId + "'"))
                         .toList();
        }

        @Override
        public void close() {
            loggerConfig.removeAppender(getName());
            context.updateLoggers();
            stop();
        }
    }
}
