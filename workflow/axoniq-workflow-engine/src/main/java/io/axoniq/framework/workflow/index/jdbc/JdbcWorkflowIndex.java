/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jdbc;

import io.axoniq.framework.workflow.runtime.execution.SegmentKeyRange;
import io.axoniq.framework.workflow.runtime.execution.WaitAssociation;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WaitClosed;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WaitOpened;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtCleared;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtSet;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowEnded;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowStarted;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges.SegmentPosition;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexRows;
import org.axonframework.common.IdentifierFactory;
import org.axonframework.common.TypeReference;
import org.axonframework.common.jdbc.JdbcException;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * JDBC implementation of {@link WorkflowIndex}, on the tables of a {@link WorkflowIndexSchema}. With the default
 * schema, it writes the same tables as the {@link io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex}. Give it the
 * {@link TransactionalExecutorProvider} of the token store, such as a
 * {@link org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider} on the
 * {@code DataSource} of a {@code JdbcTokenStore}, so the rows and the token commit together. When the index lives in
 * another database, use {@link JdbcWorkflowIndexConfiguration#storesWithToken(boolean) storesWithToken(false)}.
 * <p>
 * Create the tables with {@link #createSchema(WorkflowIndexTableFactory, ProcessingContext)}, or with a tool of your
 * own. Example usage:
 * <pre>{@code
 * WorkflowConfigurer.create().registerWorkflowIndex(config -> new JdbcWorkflowIndex(
 *         new JdbcTransactionalExecutorProvider(dataSource), config.getComponent(Converter.class),
 *         JdbcWorkflowIndexConfiguration.DEFAULT));
 * }</pre>
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection", "SqlSourceToSinkFlow"})
public class JdbcWorkflowIndex implements WorkflowIndex {

    private static final TypeReference<HashSet<String>> ASSOCIATIONS_TYPE_REF = new TypeReference<>() {
    };
    private static final int IDS_PER_QUERY = 1000;

    private final TransactionalExecutorProvider<Connection> transactionalExecutorProvider;
    private final Converter converter;
    private final boolean storesWithToken;
    private final WorkflowIndexSchema schema;

    /**
     * Creates a JDBC workflow index.
     *
     * @param transactionalExecutorProvider provides the {@link TransactionalExecutor} every statement runs through
     * @param converter                     converts the associations of a wait to and from the bytes of its row
     * @param configuration                 the configuration of this index
     */
    public JdbcWorkflowIndex(TransactionalExecutorProvider<Connection> transactionalExecutorProvider,
                             Converter converter,
                             JdbcWorkflowIndexConfiguration configuration) {
        this.transactionalExecutorProvider = requireNonNull(
                transactionalExecutorProvider, "The TransactionalExecutorProvider must not be null."
        );
        this.converter = requireNonNull(converter, "The Converter must not be null.");
        requireNonNull(configuration, "The configuration must not be null.");
        this.storesWithToken = configuration.storesWithToken();
        this.schema = configuration.schema();
    }

    /**
     * Creates the tables and indices of this index.
     *
     * @param tableFactory the factory of the statement that creates the tables and indices of the configured schema
     * @param context      the context to create them in, or {@code null} for a transaction of their own
     * @return a future completing once the tables and indices exist, or failing with a {@link JdbcException}
     */
    public CompletableFuture<Void> createSchema(WorkflowIndexTableFactory tableFactory,
                                                @Nullable ProcessingContext context) {
        return executor(context).accept(connection -> {
            try (Statement statement = tableFactory.createTableStatement(connection, schema)) {
                statement.executeBatch();
            } catch (SQLException e) {
                throw new JdbcException("Failed to create the tables or indices of the workflow index", e);
            }
        });
    }

    @Override
    public CompletableFuture<Optional<WorkflowIndexRows>> load(String module,
                                                               Segment segment,
                                                               @Nullable ProcessingContext context) {
        var range = SegmentKeyRange.of(segment);
        return executor(context).apply(connection -> {
            if (!isInitialized(connection, module)) {
                return Optional.<WorkflowIndexRows>empty();
            }
            var running = new HashSet<String>();
            var wakesAt = new HashMap<String, Instant>();
            try (var statement = prepare(connection,
                                         "SELECT " + schema.workflowIdColumn() + ", " + schema.wakesAtColumn()
                                                 + " FROM " + schema.workflowTable() + " WHERE "
                                                 + schema.workflowModuleColumn() + " = ? AND "
                                                 + schema.segmentKeyColumn() + " BETWEEN ? AND ?",
                                         module, range.from(), range.to());
                 var result = statement.executeQuery()) {
                while (result.next()) {
                    var workflowId = result.getString(1);
                    running.add(workflowId);
                    var wakes = result.getString(2);
                    if (wakes != null) {
                        wakesAt.put(workflowId, Instant.parse(wakes));
                    }
                }
            }
            List<WaitAssociation> waits = new ArrayList<>();
            try (var statement = prepare(connection,
                                         "SELECT " + schema.workflowIdColumn() + ", " + schema.stepNameColumn() + ", "
                                                 + schema.eventNameColumn() + ", " + schema.associationsColumn()
                                                 + " FROM " + schema.waitTable() + " WHERE "
                                                 + schema.workflowModuleColumn() + " = ? AND "
                                                 + schema.segmentKeyColumn() + " BETWEEN ? AND ?",
                                         module, range.from(), range.to());
                 var result = statement.executeQuery()) {
                while (result.next()) {
                    Set<String> associations = converter.convert(result.getBytes(4), ASSOCIATIONS_TYPE_REF.getType());
                    var eventName = result.getString(3);
                    waits.add(new WaitAssociation(result.getString(1),
                                                  result.getString(2),
                                                  eventName == null ? null : new QualifiedName(eventName),
                                                  associations == null ? Set.of() : associations));
                }
            }
            return Optional.of(new WorkflowIndexRows(running, wakesAt, waits));
        });
    }

    @Override
    public CompletableFuture<Void> store(String module,
                                         WorkflowIndexChanges changes,
                                         @Nullable ProcessingContext context) {
        return executor(context).accept(connection -> {
            for (WorkflowIndexChange change : changes.changes()) {
                apply(connection, module, change);
            }
            var position = changes.position();
            if (position != null) {
                movePosition(connection, module, position);
            }
        });
    }

    @Override
    public CompletableFuture<Optional<WorkflowIndexRows>> loadWorkflows(String module,
                                                                        Collection<String> workflowIds,
                                                                        @Nullable ProcessingContext context) {
        var ids = List.copyOf(workflowIds);
        return executor(context).apply(connection -> {
            if (!isInitialized(connection, module)) {
                return Optional.<WorkflowIndexRows>empty();
            }
            var running = new HashSet<String>();
            var wakesAt = new HashMap<String, Instant>();
            // In chunks, as a database limits the number of parameters of one statement.
            for (int first = 0; first < ids.size(); first += IDS_PER_QUERY) {
                var chunk = ids.subList(first, Math.min(first + IDS_PER_QUERY, ids.size()));
                try (var statement = prepare(connection,
                                             "SELECT " + schema.workflowIdColumn() + ", " + schema.wakesAtColumn()
                                                     + " FROM " + schema.workflowTable() + " WHERE "
                                                     + schema.workflowModuleColumn() + " = ? AND "
                                                     + schema.workflowIdColumn() + " IN ("
                                                     + String.join(", ", Collections.nCopies(chunk.size(), "?"))
                                                     + ")",
                                             parameters(module, chunk));
                     var result = statement.executeQuery()) {
                    while (result.next()) {
                        var workflowId = result.getString(1);
                        running.add(workflowId);
                        var wakes = result.getString(2);
                        if (wakes != null) {
                            wakesAt.put(workflowId, Instant.parse(wakes));
                        }
                    }
                }
            }
            var positions = new ArrayList<SegmentPosition>();
            try (var statement = prepare(connection,
                                         "SELECT " + schema.segmentFromColumn() + ", " + schema.segmentToColumn() + ", "
                                                 + schema.segmentPositionColumn() + " FROM " + schema.positionTable()
                                                 + " WHERE " + schema.workflowModuleColumn() + " = ?",
                                         module);
                 var result = statement.executeQuery()) {
                while (result.next()) {
                    positions.add(new SegmentPosition(new SegmentKeyRange(result.getLong(1), result.getLong(2)),
                                                      result.getLong(3)));
                }
            }
            return Optional.of(new WorkflowIndexRows(running, wakesAt, List.of(), positions));
        });
    }

    private void movePosition(Connection connection, String module, SegmentPosition position) throws SQLException {
        var range = position.range();
        // Removes the positions of the segments a split or a merge replaced: those that overlap this range.
        update(connection,
               "DELETE FROM " + schema.positionTable() + " WHERE " + schema.workflowModuleColumn() + " = ? AND "
                       + schema.segmentFromColumn() + " <= ? AND " + schema.segmentToColumn() + " >= ? AND ("
                       + schema.segmentFromColumn() + " <> ? OR " + schema.segmentToColumn() + " <> ?)",
               module, range.to(), range.from(), range.from(), range.to());
        int updated = update(connection,
                             "UPDATE " + schema.positionTable() + " SET " + schema.segmentPositionColumn() + " = ? "
                                     + "WHERE " + schema.workflowModuleColumn() + " = ? AND "
                                     + schema.segmentFromColumn() + " = ? AND " + schema.segmentToColumn() + " = ?",
                             position.position(), module, range.from(), range.to());
        if (updated == 0) {
            update(connection,
                   "INSERT INTO " + schema.positionTable() + " (" + schema.workflowModuleColumn() + ", "
                           + schema.segmentFromColumn() + ", " + schema.segmentToColumn() + ", "
                           + schema.segmentPositionColumn() + ") VALUES (?, ?, ?, ?)",
                   module, range.from(), range.to(), position.position());
        }
    }

    private static Object[] parameters(String module, List<String> workflowIds) {
        var parameters = new Object[workflowIds.size() + 1];
        parameters[0] = module;
        for (int i = 0; i < workflowIds.size(); i++) {
            parameters[i + 1] = workflowIds.get(i);
        }
        return parameters;
    }

    @Override
    public CompletableFuture<Void> initialize(String module,
                                              Set<String> runningWorkflowIds,
                                              Collection<WaitAssociation> waits) {
        return executor(null).accept(connection -> {
            var storedWorkflows = new HashSet<String>();
            try (var statement = prepare(connection,
                                         "SELECT " + schema.workflowIdColumn() + " FROM " + schema.workflowTable()
                                                 + " WHERE " + schema.workflowModuleColumn() + " = ?",
                                         module);
                 var result = statement.executeQuery()) {
                while (result.next()) {
                    storedWorkflows.add(result.getString(1));
                }
            }
            for (String workflowId : runningWorkflowIds) {
                if (!storedWorkflows.contains(workflowId)) {
                    insertWorkflow(connection, module, workflowId, null);
                }
            }
            var storedWaits = new HashSet<List<String>>();
            try (var statement = prepare(connection,
                                         "SELECT " + schema.workflowIdColumn() + ", " + schema.stepNameColumn()
                                                 + " FROM " + schema.waitTable() + " WHERE "
                                                 + schema.workflowModuleColumn() + " = ?",
                                         module);
                 var result = statement.executeQuery()) {
                while (result.next()) {
                    storedWaits.add(List.of(result.getString(1), result.getString(2)));
                }
            }
            for (WaitAssociation wait : waits) {
                if (runningWorkflowIds.contains(wait.workflowId())
                        && !storedWaits.contains(List.of(wait.workflowId(), wait.stepName()))) {
                    insertWait(connection, module, wait);
                }
            }
            if (!isInitialized(connection, module)) {
                update(connection,
                       "INSERT INTO " + schema.initializationTable() + " (" + schema.workflowModuleColumn()
                               + ") VALUES (?)",
                       module);
            }
        });
    }

    @Override
    public boolean storesWithToken() {
        return storesWithToken;
    }

    private void apply(Connection connection, String module, WorkflowIndexChange change) throws SQLException {
        switch (change) {
            case WorkflowStarted started -> {
                if (!hasWorkflow(connection, module, started.workflowId())) {
                    insertWorkflow(connection, module, started.workflowId(), null);
                }
            }
            case WorkflowEnded ended -> {
                update(connection,
                       "DELETE FROM " + schema.workflowTable() + " WHERE " + schema.workflowModuleColumn()
                               + " = ? AND " + schema.workflowIdColumn() + " = ?",
                       module, ended.workflowId());
                update(connection,
                       "DELETE FROM " + schema.waitTable() + " WHERE " + schema.workflowModuleColumn()
                               + " = ? AND " + schema.workflowIdColumn() + " = ?",
                       module, ended.workflowId());
            }
            case WaitOpened opened -> {
                var wait = opened.association();
                removeWait(connection, module, wait.workflowId(), wait.stepName());
                insertWait(connection, module, wait);
            }
            case WaitClosed closed -> removeWait(connection, module, closed.workflowId(), closed.stepName());
            case WakesAtSet set -> {
                // Added when missing: a workflow can hibernate before its start is stored.
                if (setWakesAt(connection, module, set.workflowId(), set.wakesAt().toString()) == 0) {
                    insertWorkflow(connection, module, set.workflowId(), set.wakesAt().toString());
                }
            }
            // Never added: a workflow that woke and ended must not return.
            case WakesAtCleared cleared -> setWakesAt(connection, module, cleared.workflowId(), null);
        }
    }

    private boolean isInitialized(Connection connection, String module) throws SQLException {
        try (var statement = prepare(connection,
                                     "SELECT COUNT(*) FROM " + schema.initializationTable() + " WHERE "
                                             + schema.workflowModuleColumn() + " = ?",
                                     module);
             var result = statement.executeQuery()) {
            return result.next() && result.getLong(1) > 0;
        }
    }

    private boolean hasWorkflow(Connection connection, String module, String workflowId) throws SQLException {
        try (var statement = prepare(connection,
                                     "SELECT COUNT(*) FROM " + schema.workflowTable() + " WHERE "
                                             + schema.workflowModuleColumn() + " = ? AND "
                                             + schema.workflowIdColumn() + " = ?",
                                     module, workflowId);
             var result = statement.executeQuery()) {
            return result.next() && result.getLong(1) > 0;
        }
    }

    private void insertWorkflow(Connection connection,
                                String module,
                                String workflowId,
                                @Nullable String wakesAt) throws SQLException {
        update(connection,
               "INSERT INTO " + schema.workflowTable() + " (" + schema.workflowModuleColumn() + ", "
                       + schema.workflowIdColumn() + ", " + schema.segmentKeyColumn() + ", "
                       + schema.wakesAtColumn() + ") VALUES (?, ?, ?, ?)",
               module, workflowId, SegmentKeyRange.keyOf(workflowId), wakesAt);
    }

    private int setWakesAt(Connection connection,
                           String module,
                           String workflowId,
                           @Nullable String wakesAt) throws SQLException {
        return update(connection,
                      "UPDATE " + schema.workflowTable() + " SET " + schema.wakesAtColumn() + " = ? WHERE "
                              + schema.workflowModuleColumn() + " = ? AND " + schema.workflowIdColumn() + " = ?",
                      wakesAt, module, workflowId);
    }

    private void insertWait(Connection connection, String module, WaitAssociation wait) throws SQLException {
        var eventName = wait.eventName();
        update(connection,
               "INSERT INTO " + schema.waitTable() + " (" + schema.waitIdColumn() + ", "
                       + schema.workflowModuleColumn() + ", " + schema.workflowIdColumn() + ", "
                       + schema.stepNameColumn() + ", " + schema.segmentKeyColumn() + ", "
                       + schema.eventNameColumn() + ", " + schema.associationsColumn()
                       + ") VALUES (?, ?, ?, ?, ?, ?, ?)",
               IdentifierFactory.getInstance().generateIdentifier(),
               module,
               wait.workflowId(),
               wait.stepName(),
               SegmentKeyRange.keyOf(wait.workflowId()),
               eventName == null ? null : eventName.name(),
               requireNonNull(converter.convert(new HashSet<>(wait.associations()), byte[].class),
                              "The associations must convert to bytes."));
    }

    private void removeWait(Connection connection,
                            String module,
                            String workflowId,
                            String stepName) throws SQLException {
        update(connection,
               "DELETE FROM " + schema.waitTable() + " WHERE " + schema.workflowModuleColumn() + " = ? AND "
                       + schema.workflowIdColumn() + " = ? AND " + schema.stepNameColumn() + " = ?",
               module, workflowId, stepName);
    }

    private static int update(Connection connection, String sql, @Nullable Object... parameters) throws SQLException {
        try (var statement = prepare(connection, sql, parameters)) {
            return statement.executeUpdate();
        }
    }

    private static PreparedStatement prepare(Connection connection,
                                             String sql,
                                             @Nullable Object... parameters) throws SQLException {
        var statement = connection.prepareStatement(sql);
        try {
            for (int i = 0; i < parameters.length; i++) {
                switch (parameters[i]) {
                    case null -> statement.setNull(i + 1, Types.VARCHAR);
                    case Long value -> statement.setLong(i + 1, value);
                    case byte[] value -> statement.setBytes(i + 1, value);
                    case Object value -> statement.setString(i + 1, value.toString());
                }
            }
            return statement;
        } catch (SQLException e) {
            statement.close();
            throw e;
        }
    }

    private TransactionalExecutor<Connection> executor(@Nullable ProcessingContext context) {
        return transactionalExecutorProvider.getTransactionalExecutor(context);
    }
}
