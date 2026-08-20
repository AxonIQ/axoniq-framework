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
import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Appends the engine's workflow events under a condition, so an append is rejected when the instance changed since the
 * writer last wrote to it.
 * <p>
 * Two nodes can run the same instance: one that stopped refreshing its segment claim never learns it lost it, and the
 * spawn de-duplication only sees the instances of its own process. The event store is the only state both nodes share,
 * so the check lives there.
 * <p>
 * The condition selects the instance's own events, the same criteria its state is sourced with, and is checked from the
 * position that instance last wrote at. An instance that has written nothing yet is checked from the start of the
 * store, which rejects a second node spawning the same instance.
 *
 * @author Stefan Dragisic
 * @since 0.3.0
 */
@Internal
public class WorkflowAppendConditions {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowAppendConditions.class);

    private WorkflowAppendConditions() {
        // avoid instantiation
    }

    /**
     * Appends the given {@code eventMessage} in a unit of work of its own, under the append condition of the given
     * {@code workflowExecution}, and interrupts that execution when the store rejects it.
     * <p>
     * Every workflow event the engine writes goes through here, so the condition and the response to a rejection are
     * defined in one place. A rejection means another writer recorded events for this instance, which leaves this
     * execution nothing to continue from.
     * <p>
     * The event is published without a condition when the execution carries no {@link WorkflowAppendCondition}, which
     * is the case for a mocked execution. An execution that carries one and a sink that is no {@link EventStore} is a
     * combination a start already refuses, since a condition can only be attached to an event store transaction.
     *
     * @param eventSink         the sink to publish through
     * @param unitOfWorkFactory the factory creating the unit of work the append runs in
     * @param executor          the executor to offload the append to
     * @param parentContext     the context the append's unit of work derives its resources from
     * @param eventMessage      the event to append
     * @param workflowExecution the execution appending the event
     * @return a future completing once the event is appended, failing with an
     * {@link AppendEventsTransactionRejectedException} when the instance changed since this execution last wrote
     */
    public static CompletableFuture<Void> append(EventSink eventSink,
                                                 UnitOfWorkFactory unitOfWorkFactory,
                                                 Executor executor,
                                                 Context parentContext,
                                                 EventMessage eventMessage,
                                                 WorkflowExecution workflowExecution) {
        var appendCondition = workflowExecution.appendCondition();
        if (appendCondition == null) {
            logger.debug("Publishing {} without an append condition, the execution carries none",
                         eventMessage.type());
            return publish(eventSink, unitOfWorkFactory, executor, parentContext, eventMessage,
                           workflowExecution.workflowId(), null);
        }
        if (!(eventSink instanceof EventStore eventStore)) {
            // A start refuses a sink without an event store, so this is only reachable by publishing an engine event
            // outside a started engine. Appending unconditionally would drop the fencing silently.
            throw new IllegalStateException(
                    "Cannot append " + eventMessage.type() + " of workflow '" + workflowExecution.workflowId()
                            + "' under its append condition: the event sink is a " + eventSink.getClass().getName()
                            + " instead of an event store."
            );
        }
        var criteria = EventSourcedWorkflowState.criteriaBuilder(workflowExecution.workflowId());
        return appendCondition
                .appendSequentially(position -> {
                    // No position means the instance has written nothing yet, so the check runs from the start of the
                    // store and passes only while no events exist for it.
                    var condition = position == null
                            ? AppendCondition.withCriteria(criteria)
                            : AppendCondition.withCriteria(criteria).withMarker(position);
                    var transaction = new AtomicReference<EventStoreTransaction>();
                    return publish(eventSink, unitOfWorkFactory, executor, parentContext, eventMessage,
                                   workflowExecution.workflowId(),
                                   ctx -> {
                                       // Take the transaction from the sink we publish through: transaction(context) is
                                       // keyed per event store, and a second one would append the event twice.
                                       var tx = eventStore.transaction(ctx);
                                       transaction.set(tx);
                                       tx.overrideAppendCondition(current -> condition);
                                   })
                            // The commit position is written in a nested after-commit handler, so it can only be read
                            // once the unit of work has completed.
                            .thenApply(ignored -> transaction.get().appendPosition());
                })
                .whenComplete((result, failure) -> {
                    if (isAppendRejected(failure)) {
                        // Another writer recorded events for this instance. Interrupting leaves the workflow to that
                        // writer and publishes nothing further from here.
                        logger.warn("Append of {} for workflow '{}' was rejected: another writer already recorded "
                                            + "events for this instance. Stopping this execution.",
                                    eventMessage.type(), workflowExecution.workflowId());
                        workflowExecution.interruptWorkflowDriver();
                    }
                });
    }

    /**
     * Publishes the given {@code eventMessage} in a fresh unit of work derived from the given {@code parentContext},
     * applying the given {@code conditioner} to the unit of work's context first when one is given.
     */
    private static CompletableFuture<Void> publish(EventSink eventSink,
                                                   UnitOfWorkFactory unitOfWorkFactory,
                                                   Executor executor,
                                                   Context parentContext,
                                                   EventMessage eventMessage,
                                                   String workflowId,
                                                   @Nullable Consumer<ProcessingContext> conditioner) {
        return ProcessingContextUtils
                .executeWithResult(
                        workflowId,
                        unitOfWorkFactory,
                        executor,
                        parentContext,
                        ctx -> {
                            logger.trace("Appending event {} from thread {}",
                                         eventMessage.type(),
                                         Thread.currentThread());
                            if (conditioner != null) {
                                conditioner.accept(ctx);
                            }
                            return eventSink.publish(ctx, eventMessage);
                        }
                );
    }

    /**
     * Returns whether the given {@code failure} reports an append rejected by its condition, meaning another writer
     * already recorded events for the instance.
     *
     * @param failure the failure to inspect, or {@code null} when the append succeeded.
     * @return {@code true} if the append was rejected by its condition.
     */
    public static boolean isAppendRejected(@Nullable Throwable failure) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        for (var cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
            if (cause instanceof AppendEventsTransactionRejectedException) {
                return true;
            }
        }
        return false;
    }
}
