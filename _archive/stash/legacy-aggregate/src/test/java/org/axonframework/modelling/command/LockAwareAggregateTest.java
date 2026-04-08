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

package org.axonframework.modelling.command;

import org.axonframework.common.lock.Lock;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the basics of the {@link LockAwareAggregate}.
 *
 * @author Steven van Beelen
 */
class LockAwareAggregateTest {

    @SuppressWarnings("unchecked")
    private final Aggregate<Object> mockAggregate = mock(Aggregate.class);

    private final Lock mockLock = mock(Lock.class);
    private final AtomicBoolean lockSupplierInvoked = new AtomicBoolean(false);
    private final Supplier<Lock> lockSupplier = () -> {
        lockSupplierInvoked.set(true);
        return mockLock;
    };

    private final LockAwareAggregate<Object, Aggregate<Object>> testSubject =
            new LockAwareAggregate<>(mockAggregate, lockSupplier);

    @Test
    void getWrappedAggregate() {
        assertEquals(mockAggregate, testSubject.getWrappedAggregate());
    }

    @Test
    void isLockHeld() {
        when(mockLock.isHeld()).thenReturn(true);

        assertTrue(testSubject.isLockHeld());
    }

    @Test
    void typeMethodInvokesWrappedAggregate() {
        testSubject.type();

        verify(mockAggregate).type();
    }

    @Test
    void identifierMethodInvokesWrappedAggregate() {
        testSubject.identifier();

        verify(mockAggregate).identifier();
    }

    @Test
    void versionMethodInvokesWrappedAggregate() {
        testSubject.version();

        verify(mockAggregate).version();
    }

    @Test
    void handleMethodInvokesWrappedAggregateAndInspectsLock() throws Exception {
        Message testMessage = new GenericMessage(new MessageType("message"), "some-message");
        ProcessingContext context = StubProcessingContext.forMessage(testMessage);

        testSubject.handle(testMessage, context);

        verify(mockAggregate).handle(testMessage, context);
        assertTrue(lockSupplierInvoked.get());
    }

    @Test
    void invokeMethodInvokesWrappedAggregateAndInspectsLock() {
        testSubject.invoke(someField -> "some-return");

        verify(mockAggregate).invoke(any());
        assertTrue(lockSupplierInvoked.get());
    }

    @Test
    void executeMethodInvokesWrappedAggregateAndInspectsLock() {
        testSubject.execute(someField -> {
        });

        verify(mockAggregate).execute(any());
        assertTrue(lockSupplierInvoked.get());
    }

    @Test
    void isDeletedMethodInvokesWrappedAggregate() {
        testSubject.isDeleted();

        verify(mockAggregate).isDeleted();
    }

    @Test
    void rootTypeMethodInvokesWrappedAggregate() {
        testSubject.rootType();

        verify(mockAggregate).rootType();
    }
}