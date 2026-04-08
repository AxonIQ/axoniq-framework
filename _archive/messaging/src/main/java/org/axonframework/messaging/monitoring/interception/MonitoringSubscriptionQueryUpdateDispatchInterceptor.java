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

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.core.MessageDispatchInterceptor;
import org.axonframework.messaging.core.MessageDispatchInterceptorChain;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.interception.InterceptingQueryBus;
import org.axonframework.messaging.monitoring.MessageMonitor;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;

/**
 * A {@link MessageDispatchInterceptor} that intercepts a {@link MessageDispatchInterceptorChain} of
 * {@link SubscriptionQueryUpdateMessage} and registers the {@link MessageMonitor.MonitorCallback} hooks for
 * reporting. Invoked by the {@link InterceptingQueryBus}.
 *
 * @author Mateusz Nowak
 * @since 5.0.0
 */
public class MonitoringSubscriptionQueryUpdateDispatchInterceptor implements MessageDispatchInterceptor<SubscriptionQueryUpdateMessage> {

    private final MessageMonitor<? super SubscriptionQueryUpdateMessage> messageMonitor;

    /**
     * Constructs a new MonitoringSubscriptionQueryUpdateDispatchInterceptor using the given {@link MessageMonitor}.
     *
     * @param messageMonitor The {@link MessageMonitor} instance used for reporting.
     */
    public MonitoringSubscriptionQueryUpdateDispatchInterceptor(final MessageMonitor<? super SubscriptionQueryUpdateMessage> messageMonitor) {
        this.messageMonitor = messageMonitor;
    }

    @Override
    public MessageStream<?> interceptOnDispatch(SubscriptionQueryUpdateMessage message,
                                                @Nullable ProcessingContext context,
                                                MessageDispatchInterceptorChain<SubscriptionQueryUpdateMessage> interceptorChain) {
        if (context != null && context.isStarted()) {
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
