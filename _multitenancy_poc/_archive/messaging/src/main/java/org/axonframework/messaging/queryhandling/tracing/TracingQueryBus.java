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

package org.axonframework.messaging.queryhandling.tracing;

// TODO 3594 - Introduce tracing logic here.
public class TracingQueryBus {

    /*
        @Override
    public CompletableFuture<QueryResponseMessage> query(QueryMessage query) {
        Span span = spanFactory.createQuerySpan(query, false);
        return span.runSupplier(() -> doQuery(query).whenComplete((r, t) -> {
            if (t != null) {
                span.recordException(t);
            }
        }));
    }
    */

    /*
    private ResultMessage interceptAndInvokeStreaming(
            StreamingQueryMessage query,
            MessageHandler<? super StreamingQueryMessage, ? extends QueryResponseMessage> handler, Span span) {
        try (SpanScope unused = span.makeCurrent()) {
            LegacyDefaultUnitOfWork<StreamingQueryMessage> uow = LegacyDefaultUnitOfWork.startAndGet(query);
            return uow.executeWithResult((ctx) -> {
                /*
                // TODO 3594 - Reintegrate, and construct chain only once!
                QueryHandler queryHandler = new QueryHandler() {
                    @NonNull
                    @Override
                    public MessageStream<QueryResponseMessage<?>> handle(QueryMessage<?, ?> query,
                                                                         ProcessingContext context) {
                        return handler.handle((StreamingQueryMessage<Q, R>)query, context).cast();
                    }
                };
                Object queryResponse = new QueryMessageHandlerInterceptorChain(handlerInterceptors, queryHandler)
                        .proceed(uow.getMessage(), ctx);

                 */
//    Object queryResponse = handler.handleSync(uow.getMessage(), ctx);
//                return Flux.from(query.responseType().convert(queryResponse))
//            .map(this::asResponseMessage);
//});
//        }
//        }

    /*
    UpdateEmitter logic

    @Test
    void queryUpdateEmitterIsTraced() {
        SubscriptionQueryMessage queryMessage = new GenericSubscriptionQueryMessage(
                new MessageType("chatMessages"), "some-payload",
                multipleInstancesOf(String.class), instanceOf(String.class)
        );

        UpdateHandler result = queryBus.subscribeToUpdates(
                queryMessage,
                1024
        );

        result.update().subscribe();
//        testSubject.emit(any -> true, "some-awesome-text");
        result.complete();

        spanFactory.verifySpanCompleted("QueryUpdateEmitter.scheduleQueryUpdateMessage");
        spanFactory.verifySpanHasType("QueryUpdateEmitter.scheduleQueryUpdateMessage",
                                      TestSpanFactory.TestSpanType.INTERNAL);
        spanFactory.verifySpanCompleted("QueryUpdateEmitter.emitQueryUpdateMessage");
        spanFactory.verifySpanHasType("QueryUpdateEmitter.emitQueryUpdateMessage",
                                      TestSpanFactory.TestSpanType.DISPATCH);
    }
    * */
}
