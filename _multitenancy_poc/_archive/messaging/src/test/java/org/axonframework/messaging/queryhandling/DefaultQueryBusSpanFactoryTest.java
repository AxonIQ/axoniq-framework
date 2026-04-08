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

import org.axonframework.messaging.queryhandling.tracing.DefaultQueryBusSpanFactory;
import org.axonframework.messaging.tracing.IntermediateSpanFactoryTest;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.TestSpanFactory;
import org.junit.jupiter.api.*;
import org.mockito.*;

class DefaultQueryBusSpanFactoryTest extends
        IntermediateSpanFactoryTest<DefaultQueryBusSpanFactory.Builder, DefaultQueryBusSpanFactory> {


    @Test
    void createsQuerySpanNonDistributed() {
        QueryMessage queryMessage = Mockito.mock(QueryMessage.class);
        test(spanFactory -> spanFactory.createQuerySpan(queryMessage, false),
             expectedSpan("QueryBus.query", TestSpanFactory.TestSpanType.INTERNAL)
                     .withMessage(queryMessage)
        );
    }

    @Test
    void createsQuerySpanDistributed() {
        QueryMessage queryMessage = Mockito.mock(QueryMessage.class);
        test(spanFactory -> spanFactory.createQuerySpan(queryMessage, true),
             expectedSpan("QueryBus.queryDistributed", TestSpanFactory.TestSpanType.DISPATCH)
                     .withMessage(queryMessage)
        );
    }

    @Test
    void createsSubscriptionQuerySpanNonDistributed() {
        QueryMessage queryMessage = Mockito.mock(QueryMessage.class);
        test(spanFactory -> spanFactory.createSubscriptionQuerySpan(queryMessage, false),
             expectedSpan("QueryBus.subscriptionQuery", TestSpanFactory.TestSpanType.INTERNAL)
                     .withMessage(queryMessage)
        );
    }

    @Test
    void createsSubscriptionQuerySpanDistributed() {
        QueryMessage queryMessage = Mockito.mock(QueryMessage.class);
        test(spanFactory -> spanFactory.createSubscriptionQuerySpan(queryMessage, true),
             expectedSpan("QueryBus.subscriptionQueryDistributed", TestSpanFactory.TestSpanType.DISPATCH)
                     .withMessage(queryMessage)
        );
    }

    @Test
    void createsStreamingQuerySpanNonDistributed() {
        QueryMessage queryMessage = Mockito.mock(QueryMessage.class);
        test(spanFactory -> spanFactory.createStreamingQuerySpan(queryMessage, false),
             expectedSpan("QueryBus.streamingQuery", TestSpanFactory.TestSpanType.HANDLER_CHILD)
                     .withMessage(queryMessage)
        );
    }

    @Test
    void createsStreamingQuerySpanDistributed() {
        QueryMessage queryMessage = Mockito.mock(QueryMessage.class);
        test(spanFactory -> spanFactory.createStreamingQuerySpan(queryMessage, true),
             expectedSpan("QueryBus.streamingQueryDistributed", TestSpanFactory.TestSpanType.DISPATCH)
                     .withMessage(queryMessage)
        );
    }

    @Test
    void createsQueryProcessingSpanDistributed() {
        QueryMessage queryMessage = Mockito.mock(QueryMessage.class);
        test(builder -> builder.distributedInSameTrace(true),
             spanFactory -> spanFactory.createQueryProcessingSpan(queryMessage),
             expectedSpan("QueryBus.processQueryMessage", TestSpanFactory.TestSpanType.HANDLER_CHILD)
                     .withMessage(queryMessage)
        );
    }

    @Test
    void createsQueryProcessingSpanDistributedButSeparateTrace() {
        QueryMessage queryMessage = Mockito.mock(QueryMessage.class);
        test(builder -> builder.distributedInSameTrace(false),
             spanFactory -> spanFactory.createQueryProcessingSpan(queryMessage),
             expectedSpan("QueryBus.processQueryMessage", TestSpanFactory.TestSpanType.HANDLER_LINK)
                     .withMessage(queryMessage)
        );
    }

    @Test
    void createsResponseProcessingSpan() {
        QueryMessage queryMessage = Mockito.mock(QueryMessage.class);
        test(spanFactory -> spanFactory.createResponseProcessingSpan(queryMessage),
             expectedSpan("QueryBus.processQueryResponse", TestSpanFactory.TestSpanType.INTERNAL)
                     .withMessage(queryMessage)
        );
    }

    @Test
    void propagateContext() {
        QueryMessage queryMessage = Mockito.mock(QueryMessage.class);
        testContextPropagation(queryMessage, DefaultQueryBusSpanFactory::propagateContext);
    }

    @Override
    protected DefaultQueryBusSpanFactory.Builder createBuilder(SpanFactory spanFactory) {
        return DefaultQueryBusSpanFactory.builder().spanFactory(spanFactory);
    }

    @Override
    protected DefaultQueryBusSpanFactory createFactoryBasedOnBuilder(
            DefaultQueryBusSpanFactory.Builder builder) {
        return builder.build();
    }
}