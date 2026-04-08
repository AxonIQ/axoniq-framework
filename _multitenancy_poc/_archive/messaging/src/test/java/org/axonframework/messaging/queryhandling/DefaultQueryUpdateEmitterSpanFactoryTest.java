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

package org.axonframework.messaging.queryhandling;

import org.axonframework.messaging.queryhandling.tracing.DefaultQueryUpdateEmitterSpanFactory;
import org.axonframework.messaging.tracing.IntermediateSpanFactoryTest;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.TestSpanFactory;
import org.junit.jupiter.api.*;
import org.mockito.*;

class DefaultQueryUpdateEmitterSpanFactoryTest extends
        IntermediateSpanFactoryTest<DefaultQueryUpdateEmitterSpanFactory.Builder, DefaultQueryUpdateEmitterSpanFactory> {

    @Test
    void createsDefaultScheduleSpan() {
        SubscriptionQueryUpdateMessage message = Mockito.mock(SubscriptionQueryUpdateMessage.class);
        test(spanFactory -> spanFactory.createUpdateScheduleEmitSpan(message),
             expectedSpan("QueryUpdateEmitter.scheduleQueryUpdateMessage", TestSpanFactory.TestSpanType.INTERNAL)
                     .withMessage(message)
        );
    }

    @Test
    void createsDefaultEmitSpan() {
        SubscriptionQueryUpdateMessage message = Mockito.mock(SubscriptionQueryUpdateMessage.class);
        test(spanFactory -> spanFactory.createUpdateEmitSpan(message),
             expectedSpan("QueryUpdateEmitter.emitQueryUpdateMessage", TestSpanFactory.TestSpanType.DISPATCH)
                     .withMessage(message)
        );
    }

    @Test
    void propagateContext() {
        SubscriptionQueryUpdateMessage message = Mockito.mock(SubscriptionQueryUpdateMessage.class);
        testContextPropagation(message, DefaultQueryUpdateEmitterSpanFactory::propagateContext);
    }


    @Override
    protected DefaultQueryUpdateEmitterSpanFactory.Builder createBuilder(SpanFactory spanFactory) {
        return DefaultQueryUpdateEmitterSpanFactory.builder().spanFactory(spanFactory);
    }

    @Override
    protected DefaultQueryUpdateEmitterSpanFactory createFactoryBasedOnBuilder(
            DefaultQueryUpdateEmitterSpanFactory.Builder builder) {
        return builder.build();
    }
}