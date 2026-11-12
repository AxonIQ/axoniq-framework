/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import io.axoniq.framework.integrationtests.connectionpool.PostgresServer.Database;
import io.axoniq.framework.postgresql.PostgresqlEventStorageEngine;
import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Everything one scenario needs: a fresh database, an {@link Environment} on a tiny pool, the PostgreSQL event
 * storage engine on top of it, an event store and a factory for units of work bound to the environment's transaction
 * manager.
 * <p>
 * The engine permanently holds one connection of its pool for its monitor thread, so the pool is created with one
 * connection more than the scenario asks for, and observations of the pool are made relative to that reservation.
 *
 * @author John Hendrikx
 */
final class Fixture implements AutoCloseable {

    private static final long TIMEOUT_SECONDS = 20;

    private final Database database;
    private final Environment environment;
    private final PostgresqlEventStorageEngine engine;
    private final StorageEngineBackedEventStore eventStore;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final int reservedConnections;

    private Fixture(Database database, Environment environment) {
        this.database = database;
        this.environment = environment;
        this.engine = new PostgresqlEventStorageEngine(
                environment.engineDataSource(), new DelegatingEventConverter(new JacksonConverter())
        );
        this.eventStore = new StorageEngineBackedEventStore(
                engine, new SimpleEventBus(), new AnnotationBasedTagResolver()
        );
        this.unitOfWorkFactory = new TransactionalUnitOfWorkFactory(
                environment.transactionManager(), new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE)
        );

        await().atMost(10, TimeUnit.SECONDS)
               .until(() -> environment.connectionsInUse() >= 1);

        this.reservedConnections = environment.connectionsInUse();
    }

    /**
     * Creates a fixture.
     *
     * @param setup           how to wire the transaction manager
     * @param userConnections the number of connections to leave to users of the pool
     * @param autoCommit      the auto-commit mode of the pool's connections
     * @return the fixture, never {@code null}
     * @throws Exception if the fixture could not be created
     */
    static Fixture create(TransactionSetup setup, int userConnections, boolean autoCommit) throws Exception {
        Database database = PostgresServer.newDatabase();

        database.execute("CREATE TABLE unique_email (email TEXT PRIMARY KEY)");

        return new Fixture(database, setup.create(database, userConnections, autoCommit));
    }

    Database database() {
        return database;
    }

    /**
     * Returns the number of connections of the pool that the engine holds for itself.
     *
     * @return the number of reserved connections
     */
    int reservedConnections() {
        return reservedConnections;
    }

    Environment environment() {
        return environment;
    }

    PostgresqlEventStorageEngine engine() {
        return engine;
    }

    StorageEngineBackedEventStore eventStore() {
        return eventStore;
    }

    UnitOfWork newUnitOfWork() {
        return unitOfWorkFactory.create();
    }

    /**
     * Runs the unit of work to completion.
     *
     * @param unitOfWork the unit of work
     */
    void run(UnitOfWork unitOfWork) {
        unitOfWork.execute().orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    }

    /**
     * Waits for a future, failing when it does not complete in time.
     *
     * @param future the future to wait for
     * @param <T>    the type of the result
     * @return the result
     */
    static <T> T result(CompletableFuture<T> future) {
        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    }

    /**
     * Appends an event to the event store transaction of the given context.
     *
     * @param context the processing context
     * @param userId  the user identifier, used as tag
     * @param email   the email address
     */
    void appendEvent(ProcessingContext context, String userId, String email) {
        eventStore.transaction(context).appendEvent(EventTestUtils.asEventMessage(new UserRegistered(userId, email)));
    }

    /**
     * Creates a condition for sourcing all events of the given user.
     *
     * @param userId the user identifier
     * @return the condition
     */
    static SourcingCondition sourcingConditionFor(String userId) {
        return SourcingCondition.conditionFor(EventCriteria.havingTags(new Tag("user", userId)));
    }

    /**
     * Forces the unit of work to take hold of its ambient connection, as any write through it would.
     *
     * @param context the processing context
     */
    void pinAmbientConnection(ProcessingContext context) {
        Supplier<ConnectionExecutor> supplier = context.getResource(JdbcTransactionalExecutorProvider.SUPPLIER_KEY);

        assertThat(supplier).as("the transaction manager provides an ambient connection").isNotNull();

        result(supplier.get().apply(connection -> {
            try (var statement = connection.createStatement()) {
                statement.execute("SELECT 1");
            }

            return null;
        }));
    }

    /**
     * Registers an email address through the framework's ambient connection of the unit of work, the way a store
     * that is part of the unit of work (like a token store) would write.
     *
     * @param context the processing context
     * @param email   the email address
     */
    void ambientInsertEmail(ProcessingContext context, String email) {
        Supplier<ConnectionExecutor> supplier = context.getResource(JdbcTransactionalExecutorProvider.SUPPLIER_KEY);

        assertThat(supplier).as("the transaction manager provides an ambient connection").isNotNull();

        result(supplier.get().apply(connection -> {
            try (var statement = connection.prepareStatement("INSERT INTO unique_email (email) VALUES (?)")) {
                statement.setString(1, email);
                statement.executeUpdate();
            }

            return null;
        }));
    }

    /**
     * Returns the number of events in the event store, as seen by a connection outside the pool under test.
     *
     * @return the number of events
     * @throws Exception if the number could not be determined
     */
    long storedEvents() throws Exception {
        return database.queryLong("SELECT count(*) FROM events");
    }

    /**
     * Returns whether the given email address is registered, as seen by a connection outside the pool under test.
     *
     * @param email the email address
     * @return {@code true} if registered, otherwise {@code false}
     * @throws Exception if this could not be determined
     */
    boolean isEmailRegistered(String email) throws Exception {
        return database.queryLong("SELECT count(*) FROM unique_email WHERE email = '" + email + "'") > 0;
    }

    /**
     * Verifies that all connections that were taken from the pool have been returned, and that no session is left
     * inside a transaction.
     *
     * @throws Exception if the database could not be inspected
     */
    void assertPoolRecovered() throws Exception {
        await().atMost(5, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(environment.connectionsInUse())
                       .as("connections in use, besides the %d reserved by the engine", reservedConnections)
                       .isEqualTo(reservedConnections));

        assertThat(database.idleInTransactionSessions())
                .as("sessions left idle in a transaction")
                .isZero();
    }

    @Override
    public void close() {
        try {
            Method close = engine.getClass().getDeclaredMethod("close");

            close.setAccessible(true);
            close.invoke(engine);
        }
        catch (ReflectiveOperationException e) {
            // best effort: the monitor thread would otherwise retry against a closed pool
        }

        environment.close();
    }
}
