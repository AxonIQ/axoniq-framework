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

package org.axonframework.messaging.eventhandling;

import org.axonframework.common.FutureUtils;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link DelegatingEventBus}.
 *
 * @author Mateusz Nowak
 * @author Steven van Beelen
 */
class DelegatingEventBusTest {

    private static final MessageType TEST_EVENT_TYPE = new MessageType("event");

    private EventBus delegate;
    private ProcessingContext processingContext;
    private ComponentDescriptor componentDescriptor;

    private TestDelegatingEventBus testSubject;

    @BeforeEach
    void setUp() {
        delegate = mock(EventBus.class);
        processingContext = mock(ProcessingContext.class);
        componentDescriptor = mock(ComponentDescriptor.class);

        testSubject = new TestDelegatingEventBus(delegate);
    }

    @Test
    void publishDelegatesToWrappedEventBus() {
        // given
        EventMessage testEvent = new GenericEventMessage(TEST_EVENT_TYPE, "test");
        List<EventMessage> events = List.of(testEvent);
        CompletableFuture<Void> expectedResult = FutureUtils.emptyCompletedFuture();

        when(delegate.publish(processingContext, events)).thenReturn(expectedResult);

        // when
        CompletableFuture<Void> result = testSubject.publish(processingContext, events);

        // then
        assertSame(expectedResult, result);
        verify(delegate).publish(processingContext, events);
    }

    @Test
    void publishWithNullProcessingContext() {
        // given
        EventMessage testEvent = new GenericEventMessage(TEST_EVENT_TYPE, "test");
        List<EventMessage> events = List.of(testEvent);
        CompletableFuture<Void> expectedResult = FutureUtils.emptyCompletedFuture();

        when(delegate.publish(null, events)).thenReturn(expectedResult);

        // when
        CompletableFuture<Void> result = testSubject.publish(null, events);

        // then
        assertSame(expectedResult, result);
        verify(delegate).publish(null, events);
    }

    @Test
    void subscribeDelegatesToWrappedEventBus() {
        // given
        BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer =
            (events, context) -> CompletableFuture.completedFuture(null);
        Registration expectedRegistration = mock(Registration.class);

        when(delegate.subscribe(eventsBatchConsumer)).thenReturn(expectedRegistration);

        // when
        Registration result = testSubject.subscribe(eventsBatchConsumer);

        // then
        assertSame(expectedRegistration, result);
        verify(delegate).subscribe(eventsBatchConsumer);
    }

    @Test
    void describeToDelegatesToWrappedEventBus() {
        // given / when
        testSubject.describeTo(componentDescriptor);

        // then
        verify(delegate).describeTo(componentDescriptor);
        verifyNoMoreInteractions(componentDescriptor);
    }

    /**
     * Concrete test implementation of the abstract {@link DelegatingEventBus} for testing purposes.
     */
    private static class TestDelegatingEventBus extends DelegatingEventBus {

        protected TestDelegatingEventBus(EventBus delegate) {
            super(delegate);
        }
    }
}
