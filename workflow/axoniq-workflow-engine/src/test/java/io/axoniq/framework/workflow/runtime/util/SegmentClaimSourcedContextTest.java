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
package io.axoniq.framework.workflow.runtime.util;

import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.NoTransactionManager;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * A workflow execution restored while a segment is claimed must still be able to append events once the claim callback
 * has returned.
 * <p>
 * Restoring an instance sources its events, which binds an event-store transaction to the short-lived unit of work the
 * claim uses for sourcing. The restored state keeps that unit of work's context per step, and every later step event
 * the instance publishes is sent from a fresh unit of work seeded with that context's resources. If the bound
 * transaction travels along, the append is routed back into the finished claim unit of work and fails, leaving the
 * instance restored but unable to make progress.
 */
class SegmentClaimSourcedContextTest {

    private static final MessageType STEP_EVENT_TYPE = new MessageType("waitForResumeCompleted");
    private static final Executor DIRECT = Runnable::run;
    private static final Context.ResourceKey<String> MARKER = Context.ResourceKey.withLabel("marker");

    private InMemoryEventStorageEngine storageEngine;
    private EventStore eventStore;
    private UnitOfWorkFactory unitOfWorkFactory;

    @BeforeEach
    void setUp() {
        storageEngine = new InMemoryEventStorageEngine();
        eventStore = new StorageEngineBackedEventStore(
                storageEngine,
                new SimpleEventBus(),
                event -> Set.of(Tag.of("workflowId", "wf-1"))
        );
        // The application's unit-of-work factory is transactional, exactly as an Axon application's default is.
        unitOfWorkFactory = new TransactionalUnitOfWorkFactory(
                NoTransactionManager.instance(),
                new SimpleUnitOfWorkFactory(new StubApplicationContext(eventStore))
        );
    }

    @Test
    void restoredWorkflowCanAppendAfterTheSegmentClaimUnitOfWorkCommitted() {
        var sourcedContext = restoreSegmentWorkflowsAndSourceState();

        assertThat(sourcedContext.get().isCompleted())
                .as("the claim's sourcing unit of work must be finished before the restored body appends")
                .isTrue();

        assertThatCode(() -> publishStepEventFrom(sourcedContext.get()))
                .as("a restored workflow execution must still be able to append its step events")
                .doesNotThrowAnyException();

        assertThat(storageEngine.latestToken().join())
                .as("the step event must actually reach the event store")
                .isNotNull();
    }

    @Test
    void sourcingResourcesTravelToTheNextUnitOfWorkExceptTheTransactionItself() {
        var sourcedContext = restoreSegmentWorkflowsAndSourceState().get();
        sourcedContext.putResource(MARKER, "carried");

        assertThat(sourcedContext.resources().values())
                .as("the claim's sourcing unit of work really did open a transaction")
                .hasAtLeastOneElementOfType(EventStoreTransaction.class);

        var target = new AtomicReference<ProcessingContext>();
        unitOfWorkFactory.create("StepEvent")
                         .executeWithResult(context -> {
                             target.set(ProcessingContextUtils.copyResources(sourcedContext, context));
                             return CompletableFuture.completedFuture(null);
                         })
                         .join();

        assertThat(target.get().getResource(MARKER))
                .as("plain resources must still be carried over")
                .isEqualTo("carried");
        assertThat(target.get().resources().values())
                .as("the transaction is bound to the finished unit of work and must not travel")
                .doesNotHaveAnyElementsOfTypes(EventStoreTransaction.class);
    }

    /**
     * Mirrors what the segment-claim callback does: a transactional unit of work sources the durable workflow state,
     * and the context it sourced with ends up in the restored state (per step).
     */
    private AtomicReference<ProcessingContext> restoreSegmentWorkflowsAndSourceState() {
        var sourcingContext = new AtomicReference<ProcessingContext>();
        unitOfWorkFactory.create("SegmentClaim0")
                         .executeWithResult(context -> {
                             sourcingContext.set(context);
                             return eventStore
                                     .transaction(context)
                                     .source(SourcingCondition.conditionFor(
                                             EventCriteria.havingTags(Tag.of("workflowId", "wf-1"))))
                                     .reduce(0, (count, entry) -> count + 1);
                         })
                         .join();
        return sourcingContext;
    }

    /**
     * Mirrors {@code AbstractStepExecutor#sendStepEvent}: a fresh unit of work, seeded with the resources of the
     * context recorded on the step, publishes the step's event.
     */
    private void publishStepEventFrom(ProcessingContext stepContext) {
        ProcessingContextUtils.executeWithResult(
                "wf-1",
                unitOfWorkFactory,
                DIRECT,
                stepContext,
                context -> eventStore.publish(
                        context,
                        List.of(new GenericEventMessage(STEP_EVENT_TYPE, Map.of("stepName",
                                                                                "waitForResume")))
                )
        ).join();
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
