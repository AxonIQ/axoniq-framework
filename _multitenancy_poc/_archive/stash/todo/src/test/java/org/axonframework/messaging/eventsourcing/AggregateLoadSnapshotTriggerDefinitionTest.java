/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.eventsourcing;

import org.axonframework.messaging.eventhandling.DomainEventMessage;
import org.axonframework.messaging.eventhandling.GenericDomainEventMessage;
import org.axonframework.messaging.eventsourcing.snapshotting.AggregateLoadTimeSnapshotTriggerDefinition;
import org.axonframework.messaging.eventsourcing.snapshotting.SnapshotTrigger;
import org.axonframework.messaging.eventsourcing.snapshotting.Snapshotter;
import org.axonframework.integrationtests.commandhandling.StubAggregate;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.messaging.unitofwork.LegacyDefaultUnitOfWork;
import org.axonframework.modelling.command.Aggregate;
import org.axonframework.modelling.command.inspection.AnnotatedAggregate;
import org.axonframework.modelling.command.inspection.AnnotatedAggregateMetaModelFactory;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link AggregateLoadTimeSnapshotTriggerDefinition}.
 *
 * @author Yvonne Ceelie
 */
class AggregateLoadSnapshotTriggerDefinitionTest {

    private AggregateLoadTimeSnapshotTriggerDefinition testSubject;
    private Snapshotter mockSnapshotter;
    private String aggregateIdentifier;
    private Aggregate<?> aggregate;
    private Instant now;

    @BeforeEach
    void setUp() {
        while (CurrentUnitOfWork.isStarted()) {
            CurrentUnitOfWork.get().rollback();
        }
        mockSnapshotter = mock(Snapshotter.class);
        testSubject = new AggregateLoadTimeSnapshotTriggerDefinition(mockSnapshotter, 1000);
        aggregateIdentifier = "aggregateIdentifier";
        LegacyDefaultUnitOfWork.startAndGet(new GenericMessage(new MessageType("message"), "test"));
        aggregate = AnnotatedAggregate.initialize(
                new StubAggregate(aggregateIdentifier),
                AnnotatedAggregateMetaModelFactory.inspectAggregate(StubAggregate.class),
                null
        );
        now = Instant.now();
        AggregateLoadTimeSnapshotTriggerDefinition.clock = Clock.fixed(now, ZoneId.of("UTC"));
    }

    @AfterEach
    void tearDown() {
        while (CurrentUnitOfWork.isStarted()) {
            CurrentUnitOfWork.get().rollback();
        }
    }

    @Test
    void snapshotterTriggeredOnUnitOfWorkCleanup() {
        SnapshotTrigger trigger = testSubject.prepareTrigger(aggregate.rootType());
        DomainEventMessage testEvent = new GenericDomainEventMessage(
                "type", aggregateIdentifier, 0, new MessageType("event"), "Mock contents"
        );
        AggregateLoadTimeSnapshotTriggerDefinition.clock = Clock.fixed(now.plusMillis(1001), ZoneId.of("UTC"));

        trigger.eventHandled(testEvent);

        verify(mockSnapshotter, never()).scheduleSnapshot(aggregate.rootType(), aggregateIdentifier);
        CurrentUnitOfWork.get()
                         .onCommit(uow -> verify(mockSnapshotter, never())
                                 .scheduleSnapshot(aggregate.rootType(), aggregateIdentifier));
        CurrentUnitOfWork.commit();
        verify(mockSnapshotter).scheduleSnapshot(aggregate.rootType(), aggregateIdentifier);
    }

    @Test
    void snapshotterTriggeredOnUnitOfWorkCommit() {
        SnapshotTrigger trigger = testSubject.prepareTrigger(aggregate.rootType());
        AggregateLoadTimeSnapshotTriggerDefinition.clock = Clock.fixed(now.plusMillis(1001), ZoneId.of("UTC"));

        DomainEventMessage testEvent = new GenericDomainEventMessage(
                "type", aggregateIdentifier, 0, new MessageType("event"), "Mock contents"
        );
        trigger.initializationFinished();
        trigger.eventHandled(testEvent);

        verify(mockSnapshotter, never()).scheduleSnapshot(aggregate.rootType(), aggregateIdentifier);
        CurrentUnitOfWork.commit();
        verify(mockSnapshotter).scheduleSnapshot(aggregate.rootType(), aggregateIdentifier);
    }

    @Test
    void snapshotterIsNotTriggeredOnUnitOfWorkRollbackIfEventsHandledAfterInitialization() {
        SnapshotTrigger trigger = testSubject.prepareTrigger(aggregate.rootType());
        AggregateLoadTimeSnapshotTriggerDefinition.clock = Clock.fixed(now.plusMillis(1001), ZoneId.of("UTC"));

        DomainEventMessage testEvent = new GenericDomainEventMessage(
                "type", aggregateIdentifier, 0, new MessageType("event"), "Mock contents"
        );
        trigger.initializationFinished();
        trigger.eventHandled(testEvent);

        verify(mockSnapshotter, never()).scheduleSnapshot(aggregate.rootType(), aggregateIdentifier);
        CurrentUnitOfWork.get().rollback();
        verify(mockSnapshotter, never()).scheduleSnapshot(aggregate.rootType(), aggregateIdentifier);
    }

    @Test
    void snapshotterTriggeredOnUnitOfWorkRollbackWhenEventsHandledBeforeInitialization() {
        SnapshotTrigger trigger = testSubject.prepareTrigger(aggregate.rootType());
        AggregateLoadTimeSnapshotTriggerDefinition.clock = Clock.fixed(now.plusMillis(1001), ZoneId.of("UTC"));

        DomainEventMessage testEvent = new GenericDomainEventMessage(
                "type", aggregateIdentifier, 0, new MessageType("event"), "Mock contents"
        );
        trigger.eventHandled(testEvent);
        trigger.initializationFinished();

        verify(mockSnapshotter, never()).scheduleSnapshot(aggregate.rootType(), aggregateIdentifier);
        CurrentUnitOfWork.get().rollback();
        verify(mockSnapshotter).scheduleSnapshot(aggregate.rootType(), aggregateIdentifier);
    }

    @Test
    void snapshotterNotTriggered() {
        SnapshotTrigger trigger = testSubject.prepareTrigger(aggregate.rootType());
        AggregateLoadTimeSnapshotTriggerDefinition.clock = Clock.fixed(now.plusMillis(1000), ZoneId.of("UTC"));

        DomainEventMessage testEvent = new GenericDomainEventMessage(
                "type", aggregateIdentifier, 0, new MessageType("event"), "Mock contents"
        );
        trigger.eventHandled(testEvent);

        verify(mockSnapshotter, never()).scheduleSnapshot(aggregate.getClass(), aggregateIdentifier);
        CurrentUnitOfWork.commit();
        verify(mockSnapshotter, never()).scheduleSnapshot(aggregate.getClass(), aggregateIdentifier);
    }

    @Test
    void scheduleANewSnapshotAfterCommitTrigger() {
        SnapshotTrigger trigger = testSubject.prepareTrigger(aggregate.rootType());
        AggregateLoadTimeSnapshotTriggerDefinition.clock = Clock.fixed(now.plusMillis(1001), ZoneId.of("UTC"));

        DomainEventMessage testEvent = new GenericDomainEventMessage(
                "type", aggregateIdentifier, 0, new MessageType("event"), "Mock contents"
        );
        CurrentUnitOfWork.commit();
        trigger.eventHandled(testEvent);
        verify(mockSnapshotter).scheduleSnapshot(aggregate.rootType(), aggregateIdentifier);
    }
}
