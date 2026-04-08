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
package org.axonframework.messaging.timeout;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.interception.EventMessageHandlerInterceptorChain;
import org.axonframework.messaging.core.MessageHandlerInterceptor;
import org.axonframework.messaging.core.timeout.AxonTimeoutException;
import org.axonframework.messaging.core.timeout.UnitOfWorkTimeoutInterceptorBuilder;
import org.axonframework.messaging.unitofwork.LegacyDefaultUnitOfWork;
import org.junit.jupiter.api.*;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link UnitOfWorkTimeoutInterceptorBuilder}.
 *
 * @author Mitchell Herrijgers
 */
@Disabled("TODO as part of #3559")
class UnitOfWorkTimeoutInterceptorBuilderTest {

    @Test
    void interruptsUnitOfWorkThatTakesTooLong() {
        MessageHandlerInterceptor<EventMessage> testSubject = new UnitOfWorkTimeoutInterceptorBuilder("MyUnitOfWork", 100, 50, 10).buildEventInterceptor();

        LegacyDefaultUnitOfWork<EventMessage> uow = new LegacyDefaultUnitOfWork<>(
                EventTestUtils.asEventMessage("test")
        );
        EventMessageHandlerInterceptorChain interceptorChain = new EventMessageHandlerInterceptorChain(
                Collections.singletonList(testSubject), (message, ctx) -> {
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                }
        );
        uow.executeWithResult((ctx) -> interceptorChain.proceed(uow.getMessage(), ctx));
        assertTrue(uow.isRolledBack());
        assertTrue(uow.getExecutionResult().isExceptionResult());
        assertInstanceOf(AxonTimeoutException.class, uow.getExecutionResult().getExceptionResult());
    }


    @Test
    void doesNotInterruptWorkWithinTime() {
        MessageHandlerInterceptor<EventMessage> testSubject = new UnitOfWorkTimeoutInterceptorBuilder("MyUnitOfWork", 100, 50, 10).buildEventInterceptor();

        LegacyDefaultUnitOfWork<EventMessage> uow = new LegacyDefaultUnitOfWork<>(
                EventTestUtils.asEventMessage("test")
        );
        EventMessageHandlerInterceptorChain interceptorChain = new EventMessageHandlerInterceptorChain(
                Collections.singletonList(testSubject), (message, ctx) -> {
                    try {
                        Thread.sleep(80);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                }

        );
        uow.executeWithResult((ctx) -> interceptorChain.proceed(uow.getMessage(), ctx));
        assertFalse(uow.isRolledBack());
        assertFalse(uow.getExecutionResult().isExceptionResult());
    }
}