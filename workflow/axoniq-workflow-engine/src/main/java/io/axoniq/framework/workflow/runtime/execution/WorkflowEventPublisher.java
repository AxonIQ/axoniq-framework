/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.util.ProcessingContextUtils;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Publishes a workflow event in a child unit of work and returns its committed append position.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
final class WorkflowEventPublisher {

    private final EventStore eventStore;
    private final String workflowId;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Executor executor;

    WorkflowEventPublisher(EventStore eventStore,
                           String workflowId,
                           UnitOfWorkFactory unitOfWorkFactory,
                           Executor executor) {
        this.eventStore = Objects.requireNonNull(eventStore, "EventStore is mandatory");
        this.workflowId = Objects.requireNonNull(workflowId, "Workflow id is mandatory");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "UnitOfWorkFactory is mandatory");
        this.executor = Objects.requireNonNull(executor, "Executor is mandatory");
    }

    CompletableFuture<ConsistencyMarker> publish(EventMessage event,
                                                 Context parentContext,
                                                 AppendCondition condition) {
        var transaction = new AtomicReference<EventStoreTransaction>();
        return ProcessingContextUtils.executeWithResult(
                workflowId,
                unitOfWorkFactory,
                executor,
                parentContext,
                context -> {
                    var eventStoreTransaction = eventStore.transaction(context);
                    transaction.set(eventStoreTransaction);
                    eventStoreTransaction.overrideAppendCondition(ignored -> condition);
                    return eventStore.publish(context, event);
                }
        ).thenApply(ignored -> transaction.get().appendPosition());
    }
}
