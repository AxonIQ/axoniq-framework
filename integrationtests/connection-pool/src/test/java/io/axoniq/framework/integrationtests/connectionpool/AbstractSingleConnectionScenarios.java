/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import org.awaitility.core.ConditionTimeoutException;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle.Phase;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/**
 * Scenarios that run units of work against the PostgreSQL event storage engine with a pool that leaves exactly one
 * connection to users (the engine reserves the other for its monitor thread). Each scenario states the behavior that
 * is desired; a scenario failing means that the configuration under test deadlocks, leaks a connection, or loses the
 * atomicity of the unit of work in that situation.
 * <p>
 * Subclasses provide the transaction setup to run the scenarios with.
 *
 * @author John Hendrikx
 */
@Timeout(120)
abstract class AbstractSingleConnectionScenarios {

    /**
     * A phase after the events were handed to the engine, but before the commit.
     */
    private static final Phase AFTER_APPEND_BEFORE_COMMIT = () -> EventStoreTransaction.APPEND_EVENTS.order() + 500;

    private final ExecutorService otherThreads = Executors.newCachedThreadPool(new NamedThreads("other-"));

    protected Fixture fixture;

    /**
     * Returns the transaction setup to run the scenarios with.
     *
     * @return the setup, never {@code null}
     */
    protected abstract TransactionSetup setup();

    /**
     * Returns the auto-commit mode the pool hands out connections in.
     *
     * @return {@code true} for auto-commit, which is the default of connection pools
     */
    protected boolean autoCommit() {
        return true;
    }

    @BeforeEach
    void createFixture() throws Exception {
        fixture = Fixture.create(setup(), 1, autoCommit());
    }

    @AfterEach
    void closeFixture() {
        otherThreads.shutdownNow();
        fixture.close();
    }

    private void seedEvents(String userId, int count) {
        UnitOfWork seed = fixture.newUnitOfWork();

        seed.runOnInvocation(context -> IntStream.range(0, count)
                                                 .forEach(i -> fixture.appendEvent(context, userId, i + "@seed")));

        fixture.run(seed);
    }

    @Nested
    class WriteOnly {

        @Test
        void shouldAppendEventsAndReturnTheConnection() throws Exception {
            // given
            UnitOfWork unitOfWork = fixture.newUnitOfWork();

            unitOfWork.runOnInvocation(context -> {
                fixture.appendEvent(context, "u1", "a@example.com");
                fixture.appendEvent(context, "u1", "b@example.com");
            });

            // when
            assertThatCode(() -> fixture.run(unitOfWork)).doesNotThrowAnyException();

            // then
            assertThat(fixture.storedEvents()).isEqualTo(2);
            fixture.assertPoolRecovered();
        }
    }

    @Nested
    class SourcingBeforeAppending {

        @Test
        void shouldSourceAndThenAppendInOneUnitOfWork() throws Exception {
            // given
            seedEvents("u1", 3);

            UnitOfWork unitOfWork = fixture.newUnitOfWork();

            unitOfWork.onInvocation(context -> fixture.eventStore()
                                                      .transaction(context)
                                                      .source(Fixture.sourcingConditionFor("u1"))
                                                      .reduce(0, (count, entry) -> count + 1)
                                                      .thenAccept(count -> {
                                                          assertThat(count).isEqualTo(3);
                                                          fixture.appendEvent(context, "u1", "next@example.com");
                                                      }));

            // when
            assertThatCode(() -> fixture.run(unitOfWork)).doesNotThrowAnyException();

            // then
            assertThat(fixture.storedEvents()).isEqualTo(4);
            fixture.assertPoolRecovered();
        }

        @Test
        void shouldSourceInParallelInOneUnitOfWork() throws Exception {
            // given
            seedEvents("u1", 3);

            UnitOfWork unitOfWork = fixture.newUnitOfWork();
            AtomicInteger sourcedByAll = new AtomicInteger();

            unitOfWork.onInvocation(context -> {
                List<CompletableFuture<Integer>> reads = new ArrayList<>();

                for (int i = 0; i < 4; i++) {
                    reads.add(CompletableFuture.supplyAsync(
                            () -> Fixture.result(fixture.eventStore()
                                                        .transaction(context)
                                                        .source(Fixture.sourcingConditionFor("u1"))
                                                        .reduce(0, (count, entry) -> count + 1)),
                            otherThreads
                    ));
                }

                return CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new))
                                        .thenAccept(unused -> reads.forEach(
                                                read -> sourcedByAll.addAndGet(read.join())
                                        ));
            });

            // when
            assertThatCode(() -> fixture.run(unitOfWork)).doesNotThrowAnyException();

            // then
            assertThat(sourcedByAll).hasValue(12);
            fixture.assertPoolRecovered();
        }
    }

    @Nested
    class UnitOfWorkInsideAnOuterTransaction {

        /**
         * A unit of work that takes part in a transaction that was already active does not commit anything when it
         * commits, so its after commit phase runs before the work is committed. This must not be accepted.
         */
        @Test
        void shouldRejectAUnitOfWorkThatWouldTakePartInAnOuterTransaction() throws Exception {
            // given
            UnitOfWork unitOfWork = fixture.newUnitOfWork();

            unitOfWork.runOnInvocation(context -> fixture.appendEvent(context, "u1", "a@example.com"));

            // when / then
            fixture.environment().runInOuterTransaction(
                () -> assertThatThrownBy(() -> fixture.run(unitOfWork)).hasStackTraceContaining(
                    "IllegalTransactionStateException"
                )
            );

            assertThat(fixture.storedEvents()).isZero();
            fixture.assertPoolRecovered();
        }

        /**
         * Whatever happens to such a unit of work, an event must never end up committed but without a permanent global
         * index, as streams do not see it, and nothing finalizes it until some later append happens to do so.
         */
        @Test
        void shouldNeverLeaveCommittedEventsWithoutAPermanentGlobalIndex() throws Exception {
            // given
            try (Fixture roomy = Fixture.create(setup(), 2, autoCommit())) {
                UnitOfWork unitOfWork = roomy.newUnitOfWork();

                unitOfWork.runOnInvocation(context -> roomy.appendEvent(context, "u1", "a@example.com"));

                // when
                roomy.environment().runInOuterTransaction(() -> {
                    try {
                        roomy.run(unitOfWork);
                    }
                    catch (RuntimeException e) {
                        // rejecting the unit of work is fine
                    }
                });

                // then
                assertThat(unfinalizedEventsEventually(roomy)).as("events still without a permanent index").isZero();
            }
        }

        private long unfinalizedEventsEventually(Fixture target) throws Exception {
            long unfinalized = target.database().queryLong("SELECT count(*) FROM events WHERE global_index < 0");

            for (int attempt = 0; unfinalized > 0 && attempt < 25; attempt++) {
                Thread.sleep(200);

                unfinalized = target.database().queryLong("SELECT count(*) FROM events WHERE global_index < 0");
            }

            return unfinalized;
        }
    }

    @Nested
    class ApplicationCodeUsingTheConnectionFirst {

        @Test
        void shouldSourceAfterApplicationCodeRanSqlInTheTransaction() throws Exception {
            // given
            seedEvents("u1", 3);

            UnitOfWork unitOfWork = fixture.newUnitOfWork();

            unitOfWork.onInvocation(context -> {
                // application code that the framework knows nothing about takes hold of the transaction's connection
                fixture.environment().userSql("INSERT INTO unique_email (email) VALUES (?)", "a@example.com");

                return fixture.eventStore()
                              .transaction(context)
                              .source(Fixture.sourcingConditionFor("u1"))
                              .reduce(0, (count, entry) -> count + 1)
                              .thenAccept(count -> assertThat(count).isEqualTo(3));
            });

            // when
            assertThatCode(() -> fixture.run(unitOfWork)).doesNotThrowAnyException();

            // then
            assertThat(fixture.isEmailRegistered("a@example.com")).isTrue();
            fixture.assertPoolRecovered();
        }
    }

    @Nested
    class SourcingFromAnotherThread {

        @Test
        void shouldResumeSourcingOnAnotherThreadWhenTheConnectionIsAlreadyInUse() throws Exception {
            // given
            seedEvents("u1", 120);

            UnitOfWork unitOfWork = fixture.newUnitOfWork();
            AtomicReference<String> unitOfWorkThread = new AtomicReference<>();
            AtomicInteger sourced = new AtomicInteger();
            AtomicReference<Set<String>> hopThreads = new AtomicReference<>();

            unitOfWork.onInvocation(context -> {
                unitOfWorkThread.set(Thread.currentThread().getName());
                fixture.pinAmbientConnection(context);

                HoppingMessageStream<EventMessage> stream = new HoppingMessageStream<>(
                        fixture.eventStore()
                               .transaction(context)
                               .source(Fixture.sourcingConditionFor("u1"))
                               .<EventMessage>cast(),
                        otherThreads
                );

                return stream.reduce(0, (count, entry) -> count + 1)
                             .thenAccept(count -> {
                                 sourced.set(count);
                                 hopThreads.set(stream.hopThreadNames());
                             });
            });

            // when
            assertThatCode(() -> fixture.run(unitOfWork)).doesNotThrowAnyException();

            // then
            assertThat(sourced).hasValue(120);
            assertThat(hopThreads.get()).isNotEmpty().doesNotContain(unitOfWorkThread.get());
            fixture.assertPoolRecovered();
        }

        @Test
        void shouldResumeSourcingOnAnotherThreadWhenTheConnectionIsNotYetInUse() throws Exception {
            // given
            seedEvents("u1", 120);

            UnitOfWork unitOfWork = fixture.newUnitOfWork();
            AtomicInteger sourced = new AtomicInteger();

            unitOfWork.onInvocation(context -> {
                HoppingMessageStream<EventMessage> stream = new HoppingMessageStream<>(
                        fixture.eventStore()
                               .transaction(context)
                               .source(Fixture.sourcingConditionFor("u1"))
                               .<EventMessage>cast(),
                        otherThreads
                );

                return stream.reduce(0, (count, entry) -> count + 1).thenAccept(sourced::set);
            });

            // when
            assertThatCode(() -> fixture.run(unitOfWork)).doesNotThrowAnyException();

            // then
            assertThat(sourced).hasValue(120);
            fixture.assertPoolRecovered();
        }
    }

    @Nested
    class ImmediateConsistencyCheck {

        @Test
        void shouldRegisterTheEmailAndAppendTheEventTogether() throws Exception {
            // given
            UnitOfWork unitOfWork = fixture.newUnitOfWork();

            unitOfWork.runOnInvocation(context -> fixture.appendEvent(context, "u1", "a@example.com"));
            unitOfWork.runOn(AFTER_APPEND_BEFORE_COMMIT,
                             context -> fixture.environment()
                                               .userSql("INSERT INTO unique_email (email) VALUES (?)", "a@example.com"));

            // when
            assertThatCode(() -> fixture.run(unitOfWork)).doesNotThrowAnyException();

            // then
            assertThat(fixture.storedEvents()).isEqualTo(1);
            assertThat(fixture.isEmailRegistered("a@example.com")).isTrue();
            fixture.assertPoolRecovered();
        }

        @Test
        void shouldAbortTheWholeUnitOfWorkWhenTheEmailIsAlreadyRegistered() throws Exception {
            // given
            UnitOfWork first = fixture.newUnitOfWork();

            first.runOnInvocation(context -> fixture.appendEvent(context, "u1", "a@example.com"));
            first.runOn(AFTER_APPEND_BEFORE_COMMIT,
                        context -> fixture.environment()
                                          .userSql("INSERT INTO unique_email (email) VALUES (?)", "a@example.com"));

            fixture.run(first);

            UnitOfWork second = fixture.newUnitOfWork();

            second.runOnInvocation(context -> fixture.appendEvent(context, "u2", "a@example.com"));
            second.runOn(AFTER_APPEND_BEFORE_COMMIT,
                         context -> fixture.environment()
                                           .userSql("INSERT INTO unique_email (email) VALUES (?)", "a@example.com"));

            // when
            assertThatThrownBy(() -> fixture.run(second)).isNotNull();

            // then
            assertThat(fixture.storedEvents()).as("only the event of the first unit of work").isEqualTo(1);
            fixture.assertPoolRecovered();
        }
    }

    @Nested
    class FailureHandling {

        @Test
        void shouldReturnTheConnectionWhenTheUnitOfWorkFailsAfterUsingIt() throws Exception {
            // given
            UnitOfWork unitOfWork = fixture.newUnitOfWork();

            unitOfWork.runOnInvocation(context -> {
                fixture.appendEvent(context, "u1", "a@example.com");
                fixture.pinAmbientConnection(context);

                throw new IllegalStateException("handler failed");
            });

            // when
            assertThatThrownBy(() -> fixture.run(unitOfWork)).isNotNull();

            // then
            assertThat(fixture.storedEvents()).isZero();
            fixture.assertPoolRecovered();
        }

        @Test
        void shouldRollBackWritesMadeThroughTheAmbientConnection() throws Exception {
            // given
            UnitOfWork unitOfWork = fixture.newUnitOfWork();

            unitOfWork.runOnInvocation(context -> {
                fixture.ambientInsertEmail(context, "a@example.com");

                throw new IllegalStateException("handler failed");
            });

            // when
            assertThatThrownBy(() -> fixture.run(unitOfWork)).isNotNull();

            // then
            assertThat(fixture.isEmailRegistered("a@example.com")).as("the write was rolled back").isFalse();
            fixture.assertPoolRecovered();
        }
    }

    @Nested
    class FirstUseOnAnotherThread {

        @Test
        void shouldRollBackWritesWhoseConnectionWasFirstObtainedOnAnotherThread() throws Exception {
            // given
            try (Fixture roomy = Fixture.create(setup(), 2, autoCommit())) {
                UnitOfWork unitOfWork = roomy.newUnitOfWork();

                unitOfWork.onInvocation(context -> CompletableFuture.runAsync(
                        () -> roomy.ambientInsertEmail(context, "a@example.com"), otherThreads
                ).thenRun(() -> {
                    throw new IllegalStateException("handler failed");
                }));

                // when
                assertThatThrownBy(() -> roomy.run(unitOfWork)).isNotNull();

                // then
                boolean registered = roomy.isEmailRegistered("a@example.com");

                assertSoftly(softly -> {
                    softly.assertThat(registered)
                          .as("the write was rolled back")
                          .isFalse();
                    softly.assertThatCode(roomy::assertPoolRecovered)
                          .as("all connections were returned")
                          .doesNotThrowAnyException();
                });
            }
        }
    }
}
