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

package org.axonframework.messaging.eventhandling.processing;

import org.jspecify.annotations.NonNull;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageHandlerInterceptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.interception.InterceptingEventHandlingComponent;
import org.axonframework.messaging.monitoring.MessageMonitor;
import org.axonframework.messaging.monitoring.interception.MonitoringEventHandlerInterceptor;
import org.junit.jupiter.api.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.axonframework.messaging.core.MessagingTestUtils.event;
import static org.junit.jupiter.api.Assertions.*;

class EventProcessorMonitoringTest {

    @Test
    void expectCallbackForAllMessages() throws Exception {
        List<EventMessage> events = EventTestUtils.createEvents(2);
        Set<EventMessage> pending = new HashSet<>(events);
        MessageMonitor<EventMessage> messageMonitor = (message) -> new MessageMonitor.MonitorCallback() {
            @Override
            public void reportSuccess() {
                if (!pending.contains(message)) {
                    fail("Message was presented to monitor twice: " + message + " or message was unknown.");
                }
                pending.remove(message);
            }

            @Override
            public void reportFailure(@NonNull Throwable cause) {
                fail("Test expects 'reportSuccess' to be called");
            }

            @Override
            public void reportIgnored() {
                fail("Test expects 'reportSuccess' to be called");
            }
        };

        SimpleEventHandlingComponent eventHandlingComponent = SimpleEventHandlingComponent.create("test");
        eventHandlingComponent.subscribe(new QualifiedName(Integer.class), (e, c) -> MessageStream.empty());

        // Also test that the mechanism used to call the monitor can deal with the message in the unit of work being
        // modified during processing
        MessageHandlerInterceptor<EventMessage> interceptor = (message, context, interceptorChain)
                -> interceptorChain.proceed(event(123), context);

        var interceptingEventHandlingComponent = new InterceptingEventHandlingComponent(
                List.of(new MonitoringEventHandlerInterceptor(messageMonitor), interceptor),
                eventHandlingComponent
        );

        TestEventProcessor testSubject = new TestEventProcessor(interceptingEventHandlingComponent);

        testSubject.processInBatchingUnitOfWork(events);

        assertTrue(pending.isEmpty(), "Not all events were presented to monitor");
    }

    private static class TestEventProcessor implements EventProcessor {

        private final ProcessorEventHandlingComponents processorEventHandlingComponents;

        private TestEventProcessor(EventHandlingComponent eventHandlingComponent) {
            this.processorEventHandlingComponents = new ProcessorEventHandlingComponents(
                    List.of(eventHandlingComponent)
            );
        }

        @Override
        public @NonNull String name() {
            return "test";
        }

        @Override
        public @NonNull CompletableFuture<Void> start() {
            return FutureUtils.emptyCompletedFuture();
        }

        @Override
        public @NonNull CompletableFuture<Void> shutdown() {
            return FutureUtils.emptyCompletedFuture();
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isError() {
            return false;
        }

        void processInBatchingUnitOfWork(List<? extends EventMessage> eventMessages)
                throws ExecutionException, InterruptedException, TimeoutException {
            var unitOfWork = UnitOfWorkTestUtils.aUnitOfWork();
            unitOfWork.executeWithResult(ctx -> processorEventHandlingComponents
                    .handle(eventMessages, ctx)
                    .asCompletableFuture()
            ).get(2, TimeUnit.SECONDS);
        }

        @Override
        public void describeTo(@NonNull ComponentDescriptor descriptor) {
            throw new UnsupportedOperationException("Not required for tests.");
        }
    }
}

