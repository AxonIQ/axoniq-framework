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
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndexConfiguration;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.*;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
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
 * Verifies, on the real autoconfiguration chain with Axon Server as the event store and PostgreSQL for the token store
 * and the workflow index, that a JPA {@link WorkflowIndex} is registered by default, that the workflow engine keeps
 * its rows in it, that a restarted application restores a waiting workflow from those rows, that
 * {@code axon.workflow.index.enabled=false} turns it off, and that a {@code WorkflowIndex} bean of the application
 * replaces it.
 *
 * @author Stefan Dragisic
 */
class JpaWorkflowIndexAutoConfigurationIT {

    private static final String START_EVENT = "io.axoniq.index.IndexTestStarted";
    private static final String INDEX_ENABLED = "--axon.workflow.index.enabled=true";

    // No @Container: its stop after the class would take the container of the whole build away from other tests.
    private static final AxonServerContainer axonServer = SharedAxonServerContainer.INSTANCE;
    private static PostgreSQLContainer postgres;

    @BeforeAll
    static void startInfrastructure() {
        SharedAxonServerContainer.ensureStarted();
        postgres = new PostgreSQLContainer("postgres:16.2-alpine");
        postgres.start();
    }

    @AfterAll
    static void stopDatabase() {
        postgres.stop();
    }

    @Nested
    class WithTheIndexEnabled {

        @Test
        void storesTheRowOfAWaitingWorkflowAndRemovesItWhenTheWorkflowCompletes() {
            try (var context = contextWith(WaitingWorkflowApplication.class, INDEX_ENABLED)) {
                // given
                var configuration = context.getBean(AxonConfiguration.class);
                var workflowId = newOrderId();

                // when - the workflow starts and waits for its payment
                publishStart(configuration, workflowId);

                // then - the engine stored its row through the JPA workflow index
                awaitIndexed(context, workflowId);

                // when - the payment arrives, so the workflow completes
                publishPaid(configuration, workflowId);

                // then
                awaitNotIndexed(context, workflowId);
            }
        }

        @Test
        void restoresAWaitingWorkflowFromItsRowAfterARestart() {
            // given - a workflow that waits for its payment, in an application that stops
            var workflowId = newOrderId();
            try (var context = contextWith(WaitingWorkflowApplication.class, INDEX_ENABLED)) {
                publishStart(context.getBean(AxonConfiguration.class), workflowId);
                awaitIndexed(context, workflowId);
            }

            try (var context = contextWith(WaitingWorkflowApplication.class, INDEX_ENABLED)) {
                // when - the payment arrives in the restarted application
                assertThat(indexedWorkflowIds(context)).contains(workflowId);
                publishPaid(context.getBean(AxonConfiguration.class), workflowId);

                // then - the restored workflow received it and completed
                awaitNotIndexed(context, workflowId);
            }
        }

        @Test
        void writesItsRowsWithTheTokenUnlessThePropertySaysOtherwise() {
            try (var context = contextWith(WaitingWorkflowApplication.class, INDEX_ENABLED)) {
                assertThat(context.getBean(WorkflowIndex.class).storesWithToken()).isTrue();
            }
            try (var context = contextWith(WaitingWorkflowApplication.class,
                                           INDEX_ENABLED,
                                           "--axon.workflow.index.stores-with-token=false")) {
                assertThat(context.getBean(WorkflowIndex.class).storesWithToken()).isFalse();
            }
        }

        @Test
        void yieldsToAWorkflowIndexBeanOfTheApplication() {
            try (var context = contextWith(WithOwnWorkflowIndex.class, INDEX_ENABLED)) {
                assertThat(context.getBeansOfType(WorkflowIndex.class))
                        .containsOnlyKeys("ownWorkflowIndex");
            }
        }
    }

    @Nested
    class WithWorkflowsSwitchedOff {

        @Test
        void registersNoWorkflowIndex() {
            try (var context = start(WaitingWorkflowApplication.class,
                                     INDEX_ENABLED,
                                     "--axon.workflow.enabled=false")) {
                assertThat(context.getBeansOfType(WorkflowIndex.class)).isEmpty();
            }
        }
    }

    @Nested
    class WithoutTheProperty {

        @Test
        void registersAJpaWorkflowIndexThatStoresWithTheToken() {
            try (var context = contextWith(WaitingWorkflowApplication.class)) {
                var index = context.getBean(WorkflowIndex.class);
                assertThat(index).isInstanceOf(JpaWorkflowIndex.class);
                assertThat(index.storesWithToken()).isTrue();
            }
        }
    }

    @Nested
    class WithThePropertyOff {

        @Test
        void registersNoWorkflowIndex() {
            try (var context = contextWith(WaitingWorkflowApplication.class, "--axon.workflow.index.enabled=false")) {
                assertThat(context.getBeansOfType(WorkflowIndex.class)).isEmpty();
            }
        }
    }

    /**
     * Starts the application and waits until its workflow processor stored its tokens.
     */
    private static ConfigurableApplicationContext contextWith(Class<?> application, String... properties) {
        var context = start(application, properties);
        // The workflow processor starts at the head of the stream. Once its tokens are stored, that position is
        // fixed, so an event published from here on is not skipped.
        await().atMost(30, TimeUnit.SECONDS).until(() -> storedTokens(context) > 0);
        return context;
    }

    /**
     * Starts the application on the Axon Server and the PostgreSQL database of this class. Every application of this
     * class shares both, so a later one finds the events, the tokens and the rows of an earlier one.
     */
    private static ConfigurableApplicationContext start(Class<?> application, String... properties) {
        return new SpringApplicationBuilder(application)
                .web(WebApplicationType.NONE)
                .properties(Map.<String, Object>of(
                        "axon.axonserver.servers", axonServer.getHost() + ":" + axonServer.getGrpcPort(),
                        "axon.postgresql.enabled", "false",
                        "axon.springcloud.enabled", "false",
                        "axon.multitenancy.enabled", "false",
                        "spring.datasource.url", postgres.getJdbcUrl(),
                        "spring.datasource.username", postgres.getUsername(),
                        "spring.datasource.password", postgres.getPassword(),
                        "spring.jpa.hibernate.ddl-auto", "update"
                ))
                .run(properties);
    }

    private static long storedTokens(ConfigurableApplicationContext context) {
        try (var entityManager = context.getBean(EntityManagerFactory.class).createEntityManager()) {
            return entityManager.createQuery("select count(t) from TokenEntry t", Long.class).getSingleResult();
        }
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

    private static void awaitIndexed(ConfigurableApplicationContext context, String workflowId) {
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(indexedWorkflowIds(context)).contains(workflowId));
    }

    private static void awaitNotIndexed(ConfigurableApplicationContext context, String workflowId) {
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(indexedWorkflowIds(context)).doesNotContain(workflowId));
    }

    /**
     * Returns the workflow ids of the rows in the workflow index table of the application's persistence unit.
     */
    private static List<String> indexedWorkflowIds(ConfigurableApplicationContext context) {
        try (var entityManager = context.getBean(EntityManagerFactory.class).createEntityManager()) {
            return entityManager.createQuery("select e.workflowId from WorkflowIndexEntry e", String.class)
                                .getResultList();
        }
    }

    @Configuration
    @EnableAutoConfiguration
    static class WaitingWorkflowApplication {

        @Bean
        WaitingWorkflow waitingWorkflow() {
            return new WaitingWorkflow();
        }
    }

    @Configuration
    static class WithOwnWorkflowIndex extends WaitingWorkflowApplication {

        @Bean
        WorkflowIndex ownWorkflowIndex(EntityManagerFactory entityManagerFactory, GeneralConverter converter) {
            return new JpaWorkflowIndex(new JpaTransactionalExecutorProvider(entityManagerFactory),
                                        converter,
                                        JpaWorkflowIndexConfiguration.DEFAULT);
        }
    }

    public record OrderPaid(String orderId) {

    }

    public static class WaitingWorkflow {

        @Workflow(workflowName = "WaitingWorkflow", idProperty = "orderId", startOnEventName = START_EVENT)
        public void run(SimpleWorkflowContext context) {
            var orderId = String.valueOf(context.workflowPayload().get("orderId"));
            context.awaitEvent("awaitPaid", OrderPaid.class,
                               associate(payloadProperty("orderId"), equalsTo(orderId)),
                               step -> step.timeout(Duration.ofMinutes(5)));
        }
    }
}
