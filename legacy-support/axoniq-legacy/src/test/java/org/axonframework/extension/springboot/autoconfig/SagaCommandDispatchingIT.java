/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.extension.springboot.autoconfig;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.gateway.CommandDispatcher;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.GenericTokenTableFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStore;
import org.axonframework.modelling.saga.AssociationValue;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.modelling.saga.repository.jdbc.HsqlSagaSqlSchema;
import org.axonframework.modelling.saga.repository.jdbc.JdbcSagaStore;
import org.axonframework.modelling.saga.repository.jdbc.SagaSqlSchema;
import org.axonframework.spring.stereotype.Saga;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Test class validating that a Saga can send a command end to end in a Spring Boot application with a database.
 * <p>
 * A Saga event handler runs synchronously, on the event processor's thread and inside the processor's transaction. A
 * local {@code SimpleCommandBus} handles the command on that same thread, in a unit of work of its own. The command has
 * to be handled whether the Saga sends it fire-and-forget or waits for its result, for every event processor mode and
 * for both the JPA and the JDBC stores.
 *
 * @author Mateusz Nowak
 */
class SagaCommandDispatchingIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    abstract static class SagaSendingACommand {

        @Autowired
        private ApplicationContext context;

        void assertTheCommandIsHandledAndTheSagaStored(boolean waitForResult) {
            // given
            String orderId = UUID.randomUUID().toString();
            Recorder recorder = context.getBean(Recorder.class);

            // when
            context.getBean(EventGateway.class)
                   .publish(null, new OrderPlaced(orderId, waitForResult))
                   .orTimeout(TIMEOUT.toSeconds(), TimeUnit.SECONDS)
                   .join();

            // then - the command failures are part of the message, as a fire-and-forget send does not report them
            await().atMost(TIMEOUT).untilAsserted(
                    () -> assertThat(recorder.entries("command:" + orderId))
                            .as("command failures: %s", recorder.entries("failure:" + orderId))
                            .hasSize(1)
            );
            @SuppressWarnings("unchecked")
            SagaStore<Object> sagaStore = (SagaStore<Object>) context.getBean(SagaStore.class);
            await().atMost(TIMEOUT).untilAsserted(
                    () -> assertThat(sagaStore.findSagas(ShipmentSaga.class, new AssociationValue("orderId", orderId)))
                            .hasSize(1)
            );
        }
    }

    @Nested
    @SpringBootTest(classes = {JpaContext.class, ShipmentSaga.class},
            webEnvironment = SpringBootTest.WebEnvironment.NONE)
    class WithJpa extends SagaSendingACommand {

        @Test
        void aCommandSentFireAndForgetIsHandled() {
            assertTheCommandIsHandledAndTheSagaStored(false);
        }
    }

    // A context of its own: while this Saga's event is retried, later events on the processor wait behind it.
    @Nested
    @SpringBootTest(classes = {JpaContext.class, ShipmentSaga.class},
            webEnvironment = SpringBootTest.WebEnvironment.NONE,
            properties = "spring.datasource.name=waiting-saga")
    class WithJpaAndASagaWaitingForTheResult extends SagaSendingACommand {

        @Test
        void aCommandWhoseResultTheSagaWaitsForIsHandled() {
            assertTheCommandIsHandledAndTheSagaStored(true);
        }
    }

    @Nested
    @SpringBootTest(classes = {JpaContext.class, ShipmentSaga.class},
            webEnvironment = SpringBootTest.WebEnvironment.NONE,
            properties = "axon.eventhandling.processors[ShipmentSagaProcessor].mode=subscribing")
    class WithJpaAndASubscribingProcessor extends SagaSendingACommand {

        @Test
        void aCommandSentFireAndForgetIsHandled() {
            assertTheCommandIsHandledAndTheSagaStored(false);
        }
    }

    @Nested
    @SpringBootTest(classes = {JdbcContext.class, ShipmentSaga.class},
            webEnvironment = SpringBootTest.WebEnvironment.NONE)
    class WithJdbc extends SagaSendingACommand {

        @Test
        void aCommandSentFireAndForgetIsHandled() {
            assertTheCommandIsHandledAndTheSagaStored(false);
        }
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    static class JpaContext {

        @Bean
        Recorder recorder() {
            return new Recorder();
        }

        @Bean
        ShipmentCommandHandler shipmentCommandHandler(Recorder recorder) {
            return new ShipmentCommandHandler(recorder);
        }
    }

    @ContextConfiguration
    @EnableAutoConfiguration(exclude = HibernateJpaAutoConfiguration.class)
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    static class JdbcContext {

        @Bean
        Recorder recorder() {
            return new Recorder();
        }

        @Bean
        ShipmentCommandHandler shipmentCommandHandler(Recorder recorder) {
            return new ShipmentCommandHandler(recorder);
        }

        @Bean
        SagaSqlSchema sagaSqlSchema() {
            return new HsqlSagaSqlSchema();
        }

        // Spring Boot creates no tables for the JDBC stores, so they are created before the event processors start.
        @Bean
        SmartInitializingSingleton jdbcTables(TokenStore tokenStore, JdbcSagaStore sagaStore) {
            return () -> {
                ((JdbcTokenStore) tokenStore).createSchema(GenericTokenTableFactory.INSTANCE);
                try {
                    sagaStore.createSchema();
                } catch (SQLException e) {
                    throw new IllegalStateException("Failed to create the Saga tables", e);
                }
            };
        }
    }

    static class Recorder {

        private final List<String> entries = new CopyOnWriteArrayList<>();

        void add(String entry) {
            entries.add(entry);
        }

        List<String> entries(String prefix) {
            return entries.stream().filter(entry -> entry.startsWith(prefix)).toList();
        }
    }

    static class ShipmentCommandHandler {

        private final Recorder recorder;

        ShipmentCommandHandler(Recorder recorder) {
            this.recorder = recorder;
        }

        @CommandHandler
        public void handle(ShipOrder command) {
            recorder.add("command:" + command.orderId());
        }
    }

    @Saga
    public static class ShipmentSaga {

        @StartSaga
        @SagaEventHandler(associationProperty = "orderId")
        void on(OrderPlaced event, CommandDispatcher dispatcher, Recorder recorder) {
            CompletableFuture<? extends Message> result =
                    dispatcher.send(new ShipOrder(event.orderId())).getResultMessage();
            result.whenComplete((message, failure) -> {
                if (failure != null) {
                    recorder.add("failure:" + event.orderId() + ":" + failure);
                }
            });
            if (event.waitForResult()) {
                result.orTimeout(TIMEOUT.toSeconds(), TimeUnit.SECONDS).join();
            }
        }
    }

    public record OrderPlaced(String orderId, boolean waitForResult) {

    }

    public record ShipOrder(String orderId) {

    }
}
