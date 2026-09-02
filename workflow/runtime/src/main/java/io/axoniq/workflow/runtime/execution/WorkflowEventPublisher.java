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

import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
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
 * @since 0.3.0
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
