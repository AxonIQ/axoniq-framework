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

package org.axonframework.messaging.monitoring.interception;

import org.axonframework.messaging.core.MessageHandlerInterceptor;
import org.axonframework.messaging.core.MessageHandlerInterceptorChain;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.interception.InterceptingQueryBus;
import org.axonframework.messaging.monitoring.MessageMonitor;
import org.axonframework.messaging.queryhandling.QueryMessage;

/**
 * A {@link MessageHandlerInterceptor} for {@link QueryMessage} that intercepts the
 * {@link MessageHandlerInterceptorChain} to register the {@link MessageMonitor.MonitorCallback} functions on the
 * {@link ProcessingContext} hooks.
 * <p/>
 * Invoked by the {@link InterceptingQueryBus}.
 * <p/>
 * Events are only monitored when handled.
 *
 * @author Jan Galinski
 * @since 5.0.0
 */
public class MonitoringQueryHandlerInterceptor implements MessageHandlerInterceptor<QueryMessage> {

    private final MessageMonitor<? super QueryMessage> messageMonitor;

    /**
     * Constructs a MonitoringQueryHandlerInterceptor using the given {@link MessageMonitor}.
     *
     * @param messageMonitor The {@link MessageMonitor} instance used for reporting.
     */
    public MonitoringQueryHandlerInterceptor(final MessageMonitor<? super QueryMessage> messageMonitor) {
        this.messageMonitor = messageMonitor;
    }

    @Override
    public MessageStream<?> interceptOnHandle(QueryMessage message,
                                              ProcessingContext context,
                                              MessageHandlerInterceptorChain<QueryMessage> interceptorChain) {
        if (context.isStarted()) {
            final var monitorCallback = messageMonitor.onMessageIngested(message);

            context.onError(
                    (ctx, phase, error) -> monitorCallback.reportFailure(error)
            );
            context.runOnAfterCommit(
                    ctx -> monitorCallback.reportSuccess()
            );
        }
        return interceptorChain.proceed(message, context);
    }
}
