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

package org.axonframework.messaging.eventhandling.tracing;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.tracing.IntermediateSpanFactoryTest;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.TestSpanFactory;
import org.junit.jupiter.api.*;
import org.mockito.*;

class DefaultEventBusSpanFactoryTest extends IntermediateSpanFactoryTest<DefaultEventBusSpanFactory.Builder, DefaultEventBusSpanFactory> {

    @Test
    void createCommitEventsSpan() {
        test(DefaultEventBusSpanFactory::createCommitEventsSpan,
             expectedSpan("EventBus.commitEvents", TestSpanFactory.TestSpanType.INTERNAL)
        );
    }

    @Test
    void createsQuerySpanNonDistributed() {
        EventMessage eventMessage = Mockito.mock(EventMessage.class);
        test(factory -> factory.createPublishEventSpan(eventMessage),
             expectedSpan("EventBus.publishEvent", TestSpanFactory.TestSpanType.DISPATCH)
                     .withMessage(eventMessage)
        );
    }

    @Test
    void propagateContext() {
        EventMessage eventMessage = Mockito.mock(EventMessage.class);
        testContextPropagation(eventMessage, DefaultEventBusSpanFactory::propagateContext);
    }

    @Override
    protected DefaultEventBusSpanFactory.Builder createBuilder(SpanFactory spanFactory) {
        return DefaultEventBusSpanFactory.builder().spanFactory(spanFactory);
    }

    @Override
    protected DefaultEventBusSpanFactory createFactoryBasedOnBuilder(DefaultEventBusSpanFactory.Builder builder) {
        return builder.build();
    }
}