/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowAppendCondition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.NoTransactionManager;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.annotation.Nonnull;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Checks the engine's append path against a real event store: an append conditions from the position the previous one
 * wrote at, a fresh instance may only be started once, and an execution whose instance changed underneath it is
 * stopped.
 */
class WorkflowAppendConditionsTest {

    private static final String WORKFLOW_ID = "wf-1";
    private static final Executor DIRECT = Runnable::run;

    private EventStore eventStore;
    private UnitOfWorkFactory unitOfWorkFactory;

    @BeforeEach
    void setUp() {
        var storageEngine = new InMemoryEventStorageEngine();
        eventStore = new StorageEngineBackedEventStore(
                storageEngine,
                new SimpleEventBus(),
                event -> Set.of(Tag.of(WorkflowEventTags.TAG_WORKFLOW_ID, WORKFLOW_ID))
        );
        unitOfWorkFactory = new TransactionalUnitOfWorkFactory(
                NoTransactionManager.instance(),
                new SimpleUnitOfWorkFactory(new StubApplicationContext(eventStore))
        );
    }

    @Test
    void aSecondAppendConditionsOnThePositionTheFirstOneWroteAt() {
        var execution = executionWithAppendCondition(new ConsistencyMarkerSupport());

        append(execution).join();

        assertThatCode(() -> append(execution).join())
                .as("the second append conditions on the first one's position, so its own history is no conflict")
                .doesNotThrowAnyException();
        verify(execution, never()).interruptWorkflowDriver();
    }

    @Test
    void firstAppendOfAFreshInstanceAssertsTheInstanceDoesNotExistYet() {
        var winner = executionWithAppendCondition(new ConsistencyMarkerSupport());
        append(winner).join();

        var duplicateSpawn = executionWithAppendCondition(new ConsistencyMarkerSupport());
        var failure = catchThrowable(() -> append(duplicateSpawn).join());

        assertThat(WorkflowAppendConditions.isAppendRejected(failure))
                .as("a duplicate spawn must be rejected: the winner already recorded the instance")
                .isTrue();
        verify(duplicateSpawn).interruptWorkflowDriver();
        verify(winner, never()).interruptWorkflowDriver();
        assertThat(WorkflowAppendConditions.isAppendRejected(catchThrowable(() -> append(duplicateSpawn).join())))
                .as("a rejected append records no position, so the next one is rejected the same way")
                .isTrue();
    }

    @Test
    void aForeignWriteAfterTheRestoredPositionRejectsTheAppendAndStopsTheExecution() {
        var winnerCondition = new RecordingAppendCondition();
        var winner = executionWithAppendCondition(winnerCondition);
        append(winner).join();
        // The second append is handed the position the first one wrote at, which is where a second writer restored
        // just after that event resumes from.
        append(winner).join();
        var restoredAt = winnerCondition.lastPositionSeen();

        var loserCondition = new ConsistencyMarkerSupport();
        loserCondition.updateAppendPosition(restoredAt);
        var loser = executionWithAppendCondition(loserCondition);

        // The winner writes past the position the loser restored at.
        append(winner).join();
        var failure = catchThrowable(() -> append(loser).join());

        assertThat(WorkflowAppendConditions.isAppendRejected(failure))
                .as("the instance changed after the position the loser conditions from")
                .isTrue();
        verify(loser).interruptWorkflowDriver();
        assertThatCode(() -> append(winner).join())
                .as("the winner keeps appending, it conditions from its own last position")
                .doesNotThrowAnyException();
    }

    @Test
    void siblingAppendsOfOneInstanceSerializeInsteadOfConflicting() throws Exception {
        var execution = executionWithAppendCondition(new ConsistencyMarkerSupport());
        append(execution).join();

        // Released all at once so the appends really do overlap. Each one has to be handed the position its
        // predecessor wrote at, or the store rejects it.
        var siblings = 8;
        var start = new CountDownLatch(1);
        @SuppressWarnings("unchecked")
        CompletableFuture<Void>[] results = new CompletableFuture[siblings];
        var threads = new Thread[siblings];
        for (int i = 0; i < siblings; i++) {
            var index = i;
            threads[i] = Thread.ofVirtual().start(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                results[index] = append(execution);
            });
        }
        start.countDown();
        for (var thread : threads) {
            thread.join();
        }

        for (var result : results) {
            assertThatCode(result::join)
                    .as("a sibling append must not be rejected by its concurrently committing siblings")
                    .doesNotThrowAnyException();
        }
        verify(execution, never()).interruptWorkflowDriver();
    }

    @Test
    void executionsWithoutAnAppendConditionPublishUnconditionally() {
        var winner = executionWithAppendCondition(new ConsistencyMarkerSupport());
        append(winner).join();

        var unconditioned = mock(WorkflowExecution.class);
        when(unconditioned.workflowId()).thenReturn(WORKFLOW_ID);

        assertThatCode(() -> append(unconditioned).join())
                .as("an execution carrying no append condition, like a mocked one, publishes without one")
                .doesNotThrowAnyException();
    }

    @Test
    void rejectionIsRecognizedThroughTheCauseChain() {
        var rejected = AppendEventsTransactionRejectedException.conflictingEventsDetected(ConsistencyMarker.ORIGIN);

        assertThat(WorkflowAppendConditions.isAppendRejected(rejected)).isTrue();
        assertThat(WorkflowAppendConditions.isAppendRejected(new CompletionException(rejected))).isTrue();
        assertThat(WorkflowAppendConditions.isAppendRejected(new CompletionException(new RuntimeException(rejected))))
                .isTrue();
        assertThat(WorkflowAppendConditions.isAppendRejected(new IllegalStateException("boom"))).isFalse();
        assertThat(WorkflowAppendConditions.isAppendRejected(null)).isFalse();
    }

    private CompletableFuture<Void> append(WorkflowExecution execution) {
        return WorkflowAppendConditions.append(
                eventStore,
                unitOfWorkFactory,
                DIRECT,
                Context.empty(),
                new GenericEventMessage(new MessageType("SomethingHappened"), Map.of("workflowId", WORKFLOW_ID)),
                execution
        );
    }

    /**
     * Records the position each append is handed, so a test can hand that same position to a second writer.
     */
    private static final class RecordingAppendCondition implements WorkflowAppendCondition {

        private final ConsistencyMarkerSupport delegate = new ConsistencyMarkerSupport();
        private final AtomicReference<ConsistencyMarker> lastSeen = new AtomicReference<>();

        @Override
        @Nonnull
        public CompletableFuture<Void> appendSequentially(
                @Nonnull Function<ConsistencyMarker, CompletableFuture<ConsistencyMarker>> append
        ) {
            return delegate.appendSequentially(position -> {
                lastSeen.set(position);
                return append.apply(position);
            });
        }

        @Override
        public void updateAppendPosition(ConsistencyMarker position) {
            delegate.updateAppendPosition(position);
        }

        private ConsistencyMarker lastPositionSeen() {
            return lastSeen.get();
        }
    }

    private static WorkflowExecution executionWithAppendCondition(WorkflowAppendCondition state) {
        var execution = mock(WorkflowExecution.class);
        when(execution.workflowId()).thenReturn(WORKFLOW_ID);
        when(execution.appendCondition()).thenReturn(state);
        return execution;
    }

    private record StubApplicationContext(EventStore eventStore) implements ApplicationContext {

        @Override
        public <C> C component(Class<C> type, String name) {
            if (type.isInstance(eventStore)) {
                //noinspection unchecked
                return (C) eventStore;
            }
            throw new IllegalArgumentException("No component of type " + type);
        }
    }
}
