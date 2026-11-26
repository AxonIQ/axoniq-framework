/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.springboot.workflow;

import io.axoniq.framework.testcontainer.AxonServerContainer;
import io.axoniq.framework.testcontainer.SharedAxonServerContainer;
import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.index.jdbc.JdbcWorkflowIndex;
import io.axoniq.framework.workflow.index.jdbc.PostgresWorkflowIndexTableFactory;
import io.axoniq.framework.workflow.index.jdbc.WorkflowIndexSchema;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.PostgresTokenTableFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.TokenSchema;
import org.junit.jupiter.api.*;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.payloadProperty;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies, on the real autoconfiguration chain without JPA, with Axon Server as the event store and PostgreSQL for
 * the {@link JdbcTokenStore} and the workflow index, that a {@link JdbcWorkflowIndex} is registered, that it writes its
 * rows with the token, and that a restarted application restores a waiting workflow from those rows.
 *
 * @author Stefan Dragisic
 */
class JdbcWorkflowIndexAutoConfigurationIT {

    private static final String START_EVENT = "io.axoniq.index.JdbcIndexTestStarted";

    // No @Container: its stop after the class would take the container of the whole build away from other tests.
    private static final AxonServerContainer axonServer = SharedAxonServerContainer.INSTANCE;
    private static PostgreSQLContainer postgres;

    @BeforeAll
    static void startInfrastructure() throws SQLException {
        SharedAxonServerContainer.ensureStarted();
        postgres = new PostgreSQLContainer("postgres:16.2-alpine");
        postgres.start();
        // Without JPA, no tables are created for the application: the JDBC table factories create them.
        try (var connection = connection();
             var tokenTable = PostgresTokenTableFactory.INSTANCE.createTable(connection, new TokenSchema());
             var indexTables = PostgresWorkflowIndexTableFactory.INSTANCE.createTableStatement(
                     connection, WorkflowIndexSchema.defaultSchema()
             )) {
            tokenTable.executeUpdate();
            indexTables.executeBatch();
        }
    }

    @AfterAll
    static void stopDatabase() {
        postgres.stop();
    }

    @Test
    void registersAJdbcIndexThatStoresWithTheJdbcTokenStore() {
        try (var context = contextWith(WaitingWorkflowApplication.class)) {
            assertThat(context.getBean(TokenStore.class)).isInstanceOf(JdbcTokenStore.class);
            var index = context.getBean(WorkflowIndex.class);
            assertThat(index).isInstanceOf(JdbcWorkflowIndex.class);
            assertThat(index.storesWithToken()).isTrue();
        }
    }

    @Test
    void storesTheRowOfAWaitingWorkflowAndRemovesItWhenTheWorkflowCompletes() {
        try (var context = contextWith(WaitingWorkflowApplication.class)) {
            // given
            var configuration = context.getBean(AxonConfiguration.class);
            var workflowId = newOrderId();

            // when - the workflow starts and waits for its payment
            publishStart(configuration, workflowId);

            // then - the engine stored its row through the JDBC workflow index
            awaitIndexed(workflowId);

            // when - the payment arrives, so the workflow completes
            publishPaid(configuration, workflowId);

            // then
            awaitNotIndexed(workflowId);
        }
    }

    @Test
    void restoresAWaitingWorkflowFromItsRowAfterARestart() throws SQLException {
        // given - a workflow that waits for its payment, in an application that stops
        var workflowId = newOrderId();
        try (var context = contextWith(WaitingWorkflowApplication.class)) {
            publishStart(context.getBean(AxonConfiguration.class), workflowId);
            awaitIndexed(workflowId);
        }

        try (var context = contextWith(WaitingWorkflowApplication.class)) {
            // when - the payment arrives in the restarted application
            assertThat(indexedWorkflowIds()).contains(workflowId);
            publishPaid(context.getBean(AxonConfiguration.class), workflowId);

            // then - the restored workflow received it and completed
            awaitNotIndexed(workflowId);
        }
    }

    /**
     * Starts the application and waits until its workflow processor stored its tokens.
     */
    private static ConfigurableApplicationContext contextWith(Class<?> application) {
        var context = new SpringApplicationBuilder(application)
                .web(WebApplicationType.NONE)
                .properties(Map.<String, Object>of(
                        "axon.axonserver.servers", axonServer.getHost() + ":" + axonServer.getGrpcPort(),
                        "axon.postgresql.enabled", "false",
                        "axon.springcloud.enabled", "false",
                        "axon.multitenancy.enabled", "false",
                        "spring.datasource.url", postgres.getJdbcUrl(),
                        "spring.datasource.username", postgres.getUsername(),
                        "spring.datasource.password", postgres.getPassword(),
                        "spring.autoconfigure.exclude",
                        "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,"
                                + "org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration"
                ))
                .run();
        // The workflow processor starts at the head of the stream. Once its tokens are stored, that position is
        // fixed, so an event published from here on is not skipped.
        await().atMost(30, TimeUnit.SECONDS).until(() -> count("SELECT COUNT(*) FROM TokenEntry") > 0);
        return context;
    }

    private static String newOrderId() {
        return "order-" + UUID.randomUUID();
    }

    private static void publishStart(AxonConfiguration configuration, String orderId) {
        publish(configuration, new GenericEventMessage(new MessageType(START_EVENT), Map.of("orderId", orderId)));
    }

    private static void publishPaid(AxonConfiguration configuration, String orderId) {
        var paid = new OrderPaid(orderId);
        publish(configuration, new GenericEventMessage(
                configuration.getComponent(MessageTypeResolver.class).resolveOrThrow(paid), paid
        ));
    }

    private static void publish(AxonConfiguration configuration, EventMessage event) {
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish")
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
    }

    private static void awaitIndexed(String workflowId) {
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(indexedWorkflowIds()).contains(workflowId));
    }

    private static void awaitNotIndexed(String workflowId) {
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(indexedWorkflowIds()).doesNotContain(workflowId));
    }

    private static List<String> indexedWorkflowIds() throws SQLException {
        try (var connection = connection();
             var statement = connection.prepareStatement("SELECT workflowId FROM WorkflowIndexEntry");
             var result = statement.executeQuery()) {
            var workflowIds = new ArrayList<String>();
            while (result.next()) {
                workflowIds.add(result.getString(1));
            }
            return workflowIds;
        }
    }

    private static long count(String sql) throws SQLException {
        try (var connection = connection();
             var statement = connection.prepareStatement(sql);
             var result = statement.executeQuery()) {
            return result.next() ? result.getLong(1) : 0;
        }
    }

    private static java.sql.Connection connection() throws SQLException {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @Configuration
    @EnableAutoConfiguration
    static class WaitingWorkflowApplication {

        @Bean
        WaitingWorkflow waitingWorkflow() {
            return new WaitingWorkflow();
        }
    }

    public record OrderPaid(String orderId) {

    }

    public static class WaitingWorkflow {

        @Workflow(workflowName = "JdbcWaitingWorkflow", idProperty = "orderId", startOnEventName = START_EVENT)
        public void run(SimpleWorkflowContext context) {
            var orderId = String.valueOf(context.workflowPayload().get("orderId"));
            context.awaitEvent("awaitPaid", OrderPaid.class,
                               associate(payloadProperty("orderId"), equalsTo(orderId)),
                               step -> step.timeout(Duration.ofMinutes(5)));
        }
    }
}
