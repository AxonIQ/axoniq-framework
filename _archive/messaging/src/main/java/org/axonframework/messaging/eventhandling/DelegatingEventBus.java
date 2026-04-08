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

package org.axonframework.messaging.eventhandling;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Abstract implementation of an {@link EventBus} that delegates all calls to a given delegate.
 *
 * @author Mateusz Nowak
 * @since 5.0.0
 */
@Internal
public abstract class DelegatingEventBus implements EventBus {

    protected final EventBus delegate;

    /**
     * Constructs the {@code DelegatingEventBus} with the given {@code delegate} to receive calls.
     *
     * @param delegate The {@link EventBus} instance to delegate calls to.
     */
    public DelegatingEventBus(EventBus delegate) {
        this.delegate = Objects.requireNonNull(delegate, "Delegate EventBus may not be null");
    }

    @Override
    public CompletableFuture<Void> publish(@Nullable ProcessingContext context,
                                           List<? extends EventMessage> events) {
        return delegate.publish(context, events);
    }

    @Override
    public Registration subscribe(BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer) {
        return delegate.subscribe(eventsBatchConsumer);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        delegate.describeTo(descriptor);
    }
}
