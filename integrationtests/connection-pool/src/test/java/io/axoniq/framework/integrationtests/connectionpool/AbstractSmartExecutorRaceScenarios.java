/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Hammers the decision between an independent and the unit of work's connection while application code takes the
 * unit of work's connection at the same time, with a pool that leaves a single connection to users.
 * <p>
 * The unit of work waits for its readers, so a reader that decided to use an independent connection, and then finds
 * the only connection taken by the application code that is waiting for it, would never finish.
 *
 * @author John Hendrikx
 */
@Timeout(300)
abstract class AbstractSmartExecutorRaceScenarios {

    private static final int ROUNDS = 40;
    private static final int READERS = 4;

    private final ExecutorService threads = Executors.newCachedThreadPool(new NamedThreads("race-"));

    private Fixture fixture;

    /**
     * Returns the setup with the smart ambient connection to verify.
     *
     * @return the setup, never {@code null}
     */
    protected abstract TransactionSetup smartSetup();

    @BeforeEach
    void setUp() throws Exception {
        fixture = Fixture.create(smartSetup(), 1, true);
    }

    @AfterEach
    void tearDown() {
        threads.shutdownNow();
        fixture.close();
    }

    @Test
    void shouldNeitherDeadlockNorTimeOutWhenApplicationCodeTakesTheConnectionWhileReadersDecide() throws Exception {
        // given
        AtomicInteger email = new AtomicInteger();

        // when
        assertThatCode(() -> {
            for (int round = 0; round < ROUNDS; round++) {
                UnitOfWork unitOfWork = fixture.newUnitOfWork();

                unitOfWork.onInvocation(context -> {
                    TransactionalExecutor<Connection> executor =
                            new JdbcTransactionalExecutorProvider(fixture.environment().engineDataSource())
                                    .getTransactionalExecutorPreferringIndependent(context);
                    List<CompletableFuture<Void>> readers = new ArrayList<>();

                    for (int i = 0; i < READERS; i++) {
                        readers.add(CompletableFuture.runAsync(
                                () -> Fixture.result(executor.accept(connection -> {
                                    try (var statement = connection.createStatement()) {
                                        statement.execute("SELECT 1");
                                    }
                                })),
                                threads
                        ));
                    }

                    fixture.environment().userSql(
                            "INSERT INTO unique_email (email) VALUES (?)", "race-" + email.incrementAndGet()
                    );

                    return CompletableFuture.allOf(readers.toArray(CompletableFuture[]::new));
                });

                fixture.run(unitOfWork);
            }
        }).doesNotThrowAnyException();

        // then
        fixture.assertPoolRecovered();
    }
}
