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

import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.interception.InterceptingCommandBus;
import org.axonframework.messaging.core.MessageHandlerInterceptor;
import org.axonframework.messaging.core.MessageHandlerInterceptorChain;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.monitoring.MessageMonitor;

/**
 * A {@link MessageHandlerInterceptor} for {@link CommandMessage} that intercepts the
 * {@link MessageHandlerInterceptorChain} to register the
 * {@link MessageMonitor.MonitorCallback} functions on the {@link ProcessingContext} hooks.
 * <p/>
 * Invoked by the {@link InterceptingCommandBus}.
 * <p/>
 * Commands are only monitored when handled.
 *
 * @author Jan Galinski
 * @since 5.0.0
 */
public class MonitoringCommandHandlerInterceptor implements MessageHandlerInterceptor<CommandMessage> {

    private final MessageMonitor<? super CommandMessage> messageMonitor;

    /**
     * Constructs a MonitoringCommandHandlerInterceptor using the given {@link MessageMonitor}.
     *
     * @param messageMonitor The {@link MessageMonitor} instance used for reporting.
     */
    public MonitoringCommandHandlerInterceptor(final MessageMonitor<? super CommandMessage> messageMonitor) {
        this.messageMonitor = messageMonitor;
    }

    @Override
    public MessageStream<?> interceptOnHandle(CommandMessage message,
                                              ProcessingContext context,
                                              MessageHandlerInterceptorChain<CommandMessage> interceptorChain) {
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
