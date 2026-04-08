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

package org.axonframework.messaging.commandhandling.retry;

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandHandler;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageStream.Entry;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.retry.RetryScheduler;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.axonframework.common.FutureUtils.unwrap;

/**
 * A {@code CommandBus} wrapper that will retry dispatching {@link CommandMessage commands} that resulted in a failure.
 * <p>
 * A {@link RetryScheduler} is used to determine if and how retries are performed.
 *
 * @author Allard Buijze
 * @since 5.0.0
 */
public class RetryingCommandBus implements CommandBus {

    private final CommandBus delegate;
    private final RetryScheduler retryScheduler;

    /**
     * Initialize the {@code RetryingCommandBus} to dispatch commands on given {@code delegate} and perform retries
     * using the given {@code retryScheduler}.
     *
     * @param delegate       The delegate {@code CommandBus} that will handle all dispatching and handling logic.
     * @param retryScheduler The retry scheduler to use to reschedule failed commands.
     */
    public RetryingCommandBus(CommandBus delegate,
                              RetryScheduler retryScheduler) {
        this.delegate = requireNonNull(delegate, "The command bus delegate must be null.");
        this.retryScheduler = requireNonNull(retryScheduler, "the RetryScheduler must not be null.");
    }

    @Override
    public RetryingCommandBus subscribe(QualifiedName name,
                                        CommandHandler handler) {
        delegate.subscribe(name, handler);
        return this;
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                            @Nullable ProcessingContext processingContext) {
        return dispatchToDelegate(command, processingContext)
                .exceptionallyCompose(e -> performRetry(command, processingContext, unwrap(e)));
    }

    private CompletableFuture<CommandResultMessage> dispatchToDelegate(CommandMessage command,
                                                                       ProcessingContext processingContext) {
        return delegate.dispatch(command, processingContext)
                       .thenApply(Function.identity());
    }

    private CompletableFuture<CommandResultMessage> performRetry(CommandMessage command,
                                                                 ProcessingContext processingContext,
                                                                 Throwable e) {
        return retryScheduler.scheduleRetry(command, processingContext, e, this::redispatch)
                             .first().asCompletableFuture()
                             .thenApply(Entry::message);
    }

    private MessageStream<CommandResultMessage> redispatch(CommandMessage cmd, ProcessingContext ctx) {
        return MessageStream.fromFuture(dispatchToDelegate(cmd, ctx));
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
        descriptor.describeProperty("retryScheduler", retryScheduler);
    }
}
