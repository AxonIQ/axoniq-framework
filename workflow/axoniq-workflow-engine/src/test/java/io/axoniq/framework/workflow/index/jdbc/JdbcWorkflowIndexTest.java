/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jdbc;

import io.axoniq.framework.workflow.index.suite.WorkflowIndexTestSuite;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndexConfiguration;
import io.axoniq.framework.workflow.runtime.execution.WaitAssociation;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WaitOpened;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtSet;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowStarted;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexRows;
import jakarta.persistence.Persistence;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the {@link WorkflowIndexTestSuite} against a {@link JdbcWorkflowIndex} on an in-memory database, and tests that
 * it shares its tables with the {@link JpaWorkflowIndex}.
 *
 * @author Stefan Dragisic
 */
class JdbcWorkflowIndexTest extends WorkflowIndexTestSuite {

    private static final Instant WAKES_AT = Instant.parse("2026-12-24T18:00:00Z");

    private JDBCDataSource dataSource;

    @Override
    protected WorkflowIndex createIndex() {
        // A database of its own per test, so every test starts with empty tables.
        dataSource = dataSource("jdbc:hsqldb:mem:jdbcworkflowindex-" + UUID.randomUUID());
        var index = jdbcIndex();
        index.createSchema(GenericWorkflowIndexTableFactory.INSTANCE, null).orTimeout(5, TimeUnit.SECONDS).join();
        return index;
    }

    @Override
    protected void closeIndex() {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("SHUTDOWN");
        } catch (SQLException e) {
            throw new IllegalStateException("The database did not shut down.", e);
        }
    }

    @Nested
    class TablesOfTheJpaIndex {

        @Test
        void holdTheRowsEitherIndexStores() {
            // given: the tables the JPA index creates, and both indices on them
            try (var entityManagerFactory = Persistence.createEntityManagerFactory("workflow-index")) {
                var jpa = new JpaWorkflowIndex(new JpaTransactionalExecutorProvider(entityManagerFactory),
                                               new JacksonConverter(),
                                               JpaWorkflowIndexConfiguration.DEFAULT);
                // The URL of the persistence unit.
                dataSource = dataSource("jdbc:hsqldb:mem:workflowindex");
                var jdbc = jdbcIndex();

                // when: one index initializes and stores rows, and the other stores more
                jpa.initialize("orders", Set.of("order-1"), List.of(wait("order-1"))).orTimeout(5, TimeUnit.SECONDS)
                   .join();
                jdbc.store("orders", new WorkflowIndexChanges(List.of(
                        new WorkflowStarted("order-2"),
                        new WaitOpened(wait("order-2")),
                        new WakesAtSet("order-1", WAKES_AT)
                )), null).orTimeout(5, TimeUnit.SECONDS).join();

                // then: both read the same rows
                assertSameRows(load(jpa), load(jdbc));
                assertThat(load(jdbc).runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-2");
                assertThat(load(jdbc).wakesAt()).containsEntry("order-1", WAKES_AT);
                assertThat(load(jdbc).waits()).extracting(WaitAssociation::workflowId)
                                              .containsExactlyInAnyOrder("order-1", "order-2");
            }
        }

        private static void assertSameRows(WorkflowIndexRows jpa, WorkflowIndexRows jdbc) {
            assertThat(jdbc.runningWorkflowIds()).isEqualTo(jpa.runningWorkflowIds());
            assertThat(jdbc.wakesAt()).isEqualTo(jpa.wakesAt());
            assertThat(jdbc.waits()).containsExactlyInAnyOrderElementsOf(jpa.waits());
        }

        private static WaitAssociation wait(String workflowId) {
            return new WaitAssociation(workflowId, "awaitPaid", new QualifiedName("paid"),
                                       Set.of("payload:orderId=" + workflowId));
        }

        private static WorkflowIndexRows load(WorkflowIndex index) {
            return index.load("orders", Segment.ROOT_SEGMENT, null).orTimeout(5, TimeUnit.SECONDS).join().orElseThrow();
        }
    }

    private JdbcWorkflowIndex jdbcIndex() {
        return new JdbcWorkflowIndex(new JdbcTransactionalExecutorProvider(dataSource),
                                     new JacksonConverter(),
                                     JdbcWorkflowIndexConfiguration.DEFAULT);
    }

    private static JDBCDataSource dataSource(String url) {
        var dataSource = new JDBCDataSource();
        dataSource.setUrl(url);
        dataSource.setUser("sa");
        dataSource.setPassword("");
        return dataSource;
    }
}
