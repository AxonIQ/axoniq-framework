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
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkflowEventPublisherTest {

    @Test
    void publishesInAChildUnitOfWorkWithTheSuppliedConditionAndReturnsItsCommittedPosition() {
        var eventStore = mock(EventStore.class);
        var transaction = mock(EventStoreTransaction.class);
        var condition = AppendCondition.none();
        var committedPosition = mock(ConsistencyMarker.class);
        EventMessage event = new GenericEventMessage(new MessageType("WorkflowEvent"), Map.of());
        when(eventStore.transaction(any(ProcessingContext.class))).thenReturn(transaction);
        when(eventStore.publish(any(ProcessingContext.class), any(EventMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(transaction.appendPosition()).thenReturn(committedPosition);
        var publisher = new WorkflowEventPublisher(
                eventStore,
                "workflow-id",
                new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE),
                Runnable::run
        );

        var position = publisher.publish(event, Context.empty(), condition).join();

        assertThat(position).isSameAs(committedPosition);
        var order = inOrder(eventStore, transaction);
        order.verify(eventStore).transaction(any(ProcessingContext.class));
        order.verify(transaction).overrideAppendCondition(any());
        order.verify(eventStore).publish(any(ProcessingContext.class), any(EventMessage.class));
    }
}
