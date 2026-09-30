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

package org.axonframework.modelling.saga;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.util.Optional;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import static org.axonframework.messaging.eventhandling.EventTestUtils.asEventMessage;
import static org.axonframework.modelling.util.ConcurrencyUtils.testConcurrent;
import static org.junit.jupiter.api.Assertions.*;

class PayloadAssociationResolverTest {

    private PayloadAssociationResolver testSubject;
    private MessageHandlingMember<Object> handlingMember;
    private static final String TEST_PROPERTY_NAME = "testProperty";
    private static final int TEST_PROPERTY_VALUE = 42;

    @BeforeEach
    void setup() {
        testSubject = new PayloadAssociationResolver();
        handlingMember = new TestHandlingMember();
    }

    @Test
    void setTestPropertyValueIsReturnedFromResolve() {
        testResolveOnce();
    }

    @Test
    void resolveWorksThreadSafe() {
        testConcurrent(4, this::testResolveOnce);
    }

    private void testResolveOnce() {
        EventMessage eventMessage = asEventMessage(new TestEvent(TEST_PROPERTY_VALUE));
        Object result = testSubject.resolve(TEST_PROPERTY_NAME, eventMessage, handlingMember);
        assertEquals(TEST_PROPERTY_VALUE, result);
    }

    private record TestEvent(int testProperty) {

    }

    /**
     * Sleep added to {@link #payloadType()} such that the
     * {@link PayloadAssociationResolverTest#resolveWorksThreadSafe()} test would always fail with a non thread safe
     * map.
     */
    private static class TestHandlingMember implements MessageHandlingMember<Object> {

        @Override
        public @NonNull Class<?> payloadType() {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return TestEvent.class;
        }

        @Override
        public <H> Optional<H> unwrap(Class<H> handlerType) {
            return Optional.empty();
        }

        @Override
        public @NonNull MessageStream<?> handle(@NonNull Message message, @NonNull ProcessingContext context,
                                       @Nullable Object target) {
            return MessageStream.empty();
        }

        @Override
        public boolean canHandleMessageType(@NonNull Class<? extends Message> messageType) {
            return true;
        }

        @Override
        public boolean canHandle(@NonNull Message message, @NonNull ProcessingContext context) {
            return true;
        }
    }
}
