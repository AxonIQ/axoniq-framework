/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies whether two reads of one unit of work get to run in parallel, each on a connection of its own, for
 * various ways of setting up the ambient connection.
 *
 * @author John Hendrikx
 */
@Timeout(120)
abstract class AbstractParallelReadScenarios {

    private final ExecutorService threads = Executors.newCachedThreadPool(new NamedThreads("parallel-reads-"));

    /**
     * Returns the setup with the smart ambient connection to verify.
     *
     * @return the setup, never {@code null}
     */
    protected abstract TransactionSetup smartSetup();

    @AfterEach
    void tearDown() {
        threads.shutdownNow();
    }

    /**
     * What two simultaneous reads experienced.
     *
     * @param overlapped whether both were running at the same time
     * @param pids       the backend process identifiers of the connections they used
     */
    private record Outcome(boolean overlapped, List<Integer> pids) {
    }

    private Outcome readTwiceInParallel(TransactionSetup setup, boolean applicationCodePinsFirst) throws Exception {
        try (Fixture fixture = Fixture.create(setup, 2, true)) {
            UnitOfWork unitOfWork = fixture.newUnitOfWork();
            AtomicReference<Outcome> outcome = new AtomicReference<>();

            unitOfWork.onInvocation(context -> {
                if (applicationCodePinsFirst) {
                    fixture.environment().userSql("INSERT INTO unique_email (email) VALUES (?)", "a@example.com");
                }

                TransactionalExecutor<Connection> executor =
                        new JdbcTransactionalExecutorProvider(fixture.environment().engineDataSource())
                                .getTransactionalExecutorPreferringIndependent(context);
                CountDownLatch bothRunning = new CountDownLatch(2);

                CompletableFuture<Object[]> first = CompletableFuture.supplyAsync(
                        () -> Fixture.result(read(executor, bothRunning)), threads
                );
                CompletableFuture<Object[]> second = CompletableFuture.supplyAsync(
                        () -> Fixture.result(read(executor, bothRunning)), threads
                );

                return first.thenCombine(second, (a, b) -> {
                    outcome.set(new Outcome(
                            (boolean) a[0] && (boolean) b[0], List.of((int) a[1], (int) b[1])
                    ));

                    return null;
                });
            });

            fixture.run(unitOfWork);
            fixture.assertPoolRecovered();

            return outcome.get();
        }
    }

    private static CompletableFuture<Object[]> read(TransactionalExecutor<Connection> executor,
                                                    CountDownLatch bothRunning) {
        return executor.apply(connection -> {
            int pid;

            try (var statement = connection.createStatement();
                 var resultSet = statement.executeQuery("SELECT pg_backend_pid()")) {
                resultSet.next();
                pid = resultSet.getInt(1);
            }

            bothRunning.countDown();

            boolean overlapped = bothRunning.await(1500, TimeUnit.MILLISECONDS);

            return new Object[] {overlapped, pid};
        });
    }

    @Test
    void shouldReadOnSeparateConnectionsWhileNothingTookTheUnitOfWorkConnection() throws Exception {
        // given / when
        Outcome outcome = readTwiceInParallel(smartSetup(), false);

        // then
        assertThat(outcome.overlapped()).as("both reads ran at the same time").isTrue();
        assertThat(outcome.pids()).as("on separate connections").doesNotHaveDuplicates();
    }

    @Test
    void shouldShareTheUnitOfWorkConnectionOnceApplicationCodeTookIt() throws Exception {
        // given / when
        Outcome outcome = readTwiceInParallel(smartSetup(), true);

        // then
        assertThat(outcome.overlapped()).as("the reads were serialized").isFalse();
        assertThat(outcome.pids()).as("on the connection of the unit of work").containsOnly(outcome.pids().get(0));
    }

    @Test
    void shouldNeverReadInParallelWhenTheAmbientConnectionIsActiveFromTheStart() throws Exception {
        // given / when
        Outcome outcome = readTwiceInParallel(SpringJdbcSetups.LAZY_PROXY, false);

        // then
        assertThat(outcome.overlapped()).as("the reads were serialized").isFalse();
        assertThat(outcome.pids()).as("on the connection of the unit of work").containsOnly(outcome.pids().get(0));
    }

    @Test
    void shouldHoldAPhysicalConnectionOnlyFromAppendingUntilTheCommit() throws Exception {
        // given
        try (Fixture fixture = Fixture.create(smartSetup(), 2, true)) {
            UnitOfWork seed = fixture.newUnitOfWork();

            seed.runOnInvocation(context -> fixture.appendEvent(context, "u1", "seed@example.com"));
            fixture.run(seed);

            UnitOfWork unitOfWork = fixture.newUnitOfWork();
            AtomicInteger heldAfterSourcing = new AtomicInteger(-1);
            AtomicInteger heldBeforeAppending = new AtomicInteger(-1);
            AtomicInteger heldAfterAppending = new AtomicInteger(-1);
            ProcessingLifecycle.Phase afterAppending = () -> EventStoreTransaction.APPEND_EVENTS.order() + 1;

            unitOfWork.onInvocation(context -> fixture.eventStore()
                                                      .transaction(context)
                                                      .source(Fixture.sourcingConditionFor("u1"))
                                                      .reduce(0, (count, entry) -> count + 1)
                                                      .thenAccept(count -> {
                                                          heldAfterSourcing.set(heldByUnitOfWork(fixture));
                                                          fixture.appendEvent(context, "u1", "next@example.com");
                                                      }));
            unitOfWork.runOnPrepareCommit(context -> heldBeforeAppending.set(heldByUnitOfWork(fixture)));
            unitOfWork.runOn(afterAppending, context -> heldAfterAppending.set(heldByUnitOfWork(fixture)));

            // when
            fixture.run(unitOfWork);

            // then
            assertThat(heldAfterSourcing).as("connections held after sourcing").hasValue(0);
            assertThat(heldBeforeAppending).as("connections held before appending").hasValue(0);
            assertThat(heldAfterAppending).as("connections held after appending").hasValue(1);
            fixture.assertPoolRecovered();
        }
    }

    private static int heldByUnitOfWork(Fixture fixture) {
        return fixture.environment().connectionsInUse() - fixture.reservedConnections();
    }
}
