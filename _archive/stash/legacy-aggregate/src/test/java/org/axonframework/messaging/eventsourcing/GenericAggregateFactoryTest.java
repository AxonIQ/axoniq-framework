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
import org.axonframework.messaging.core.MessageType;
import org.axonframework.modelling.command.StubAggregate;
import org.axonframework.common.util.MockException;
import org.junit.jupiter.api.*;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link GenericAggregateFactory}.
 *
 * @author Allard Buijze
 */
class GenericAggregateFactoryTest {

    @Test
    void initializeRepository_NoSuitableConstructor() {
        assertThrows(IncompatibleAggregateException.class,
                     () -> new GenericAggregateFactory<>(UnsuitableAggregate.class));
    }

    @Test
    void initializeRepository_ConstructorNotCallable() {
        GenericAggregateFactory<ExceptionThrowingAggregate> factory =
                new GenericAggregateFactory<>(ExceptionThrowingAggregate.class);
        DomainEventMessage testEvent = new GenericDomainEventMessage(
                "type", "", 0, new MessageType("event"), new Object()
        );
        try {
            factory.createAggregateRoot(UUID.randomUUID().toString(), testEvent);
            fail("Expected IncompatibleAggregateException");
        } catch (IncompatibleAggregateException e) {
            // we got it
        }
    }

    @Test
    void initializeFromAggregateSnapshot() {
        StubAggregate aggregate = new StubAggregate("stubId");
        DomainEventMessage snapshotMessage = new GenericDomainEventMessage(
                "type", aggregate.getIdentifier(), 2, new MessageType("event"), aggregate
        );
        GenericAggregateFactory<StubAggregate> factory = new GenericAggregateFactory<>(StubAggregate.class);
        assertSame(aggregate, factory.createAggregateRoot(aggregate.getIdentifier(), snapshotMessage));
    }

    /**
     * Verify that {@link GenericAggregateFactory#doCreateAggregate} is not called unnecessarily.
     */
    @Test
    void initializeFromAggregateSnapshot_AvoidCallingDoCreateAggregate() {
        StubAggregate aggregate = new StubAggregate("stubId");
        DomainEventMessage snapshotMessage = new GenericDomainEventMessage(
                "type", aggregate.getIdentifier(), 2, new MessageType("event"), aggregate
        );
        AggregateFactory<StubAggregate> factory = new RogueAggregateFactory(StubAggregate.class);
        assertSame(aggregate, factory.createAggregateRoot(aggregate.getIdentifier(), snapshotMessage));
    }

    private static class UnsuitableAggregate {

        private UnsuitableAggregate(@SuppressWarnings("unused") Object uuid) {
        }
    }

    private static class ExceptionThrowingAggregate {

        private ExceptionThrowingAggregate() {
            throw new MockException();
        }
    }

    private static class RogueAggregateFactory extends GenericAggregateFactory<StubAggregate> {

        public RogueAggregateFactory(Class<StubAggregate> aggregateType) {
            super(aggregateType);
        }

        @Override
        protected StubAggregate doCreateAggregate(String aggregateIdentifier, DomainEventMessage firstEvent) {
            throw new AssertionError("Forced error");
        }
    }
}
