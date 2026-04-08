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
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.messaging.core.MessageDispatchInterceptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Decorator around the {@link EventBus} interceptors all {@link EventMessage events} before they are
 * {@link #publish(ProcessingContext, List) published} with {@link MessageDispatchInterceptor dispatch interceptors}.
 * <p>
 * This {@code InterceptingEventBus} is typically registered as a
 * {@link ComponentRegistry#registerDecorator(DecoratorDefinition) decorator} and automatically kicks in whenever
 * {@code MessageDispatchInterceptors} are present.
 *
 * @author Mateusz Nowak
 * @author Steven van Beelen
 * @since 5.0.0
 */
@Internal
public class InterceptingEventBus implements EventBus {

    /**
     * The order in which the {@link InterceptingEventBus} is applied as a
     * {@link ComponentRegistry#registerDecorator(DecoratorDefinition) decorator} to the {@link EventBus}.
     * <p>
     * As such, any decorator with a lower value will be applied to the delegate, and any higher value will be applied
     * to the {@code InterceptingEventBus} itself. Using the same value can either lead to application of the
     * decorator to the delegate or the {@code InterceptingEventBus}, depending on the order of registration.
     * <p>
     * The order of the {@code InterceptingEventBus} is set to {@code Integer.MIN_VALUE + 100} to ensure it is applied
     * very early in the configuration process, but not the earliest to allow for other decorators to be applied.
     */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 100;

    private final EventBus delegate;
    private final List<MessageDispatchInterceptor<? super EventMessage>> interceptors;
    private final InterceptingEventSink delegateSink;

    /**
     * Constructs a {@code InterceptingEventBus}, delegating all operations to the given {@code delegate}.
     * <p>
     * The given {@code interceptors} are invoked before {@link #publish(ProcessingContext, List) publishing} is done
     * by the given {@code delegate}.
     *
     * @param delegate     The delegate {@code EventBus} that will handle all dispatching and handling logic.
     * @param interceptors The interceptors to invoke before publishing an event.
     */
    @Internal
    public InterceptingEventBus(EventBus delegate,
                                List<MessageDispatchInterceptor<? super EventMessage>> interceptors) {
        this.delegate = Objects.requireNonNull(delegate, "The EventBus may not be null.");
        this.interceptors = Objects.requireNonNull(interceptors, "The dispatch interception must not be null.");
        this.delegateSink = new InterceptingEventSink(delegate, interceptors);
    }

    @Override
    public CompletableFuture<Void> publish(@Nullable ProcessingContext context,
                                           List<? extends EventMessage> events) {
        return delegateSink.publish(context, events);
    }

    @Override
    public Registration subscribe(BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer) {
        return delegate.subscribe(eventsBatchConsumer);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
        descriptor.describeProperty("dispatchInterceptors", interceptors);
        descriptor.describeProperty("delegateSink", delegateSink);
    }
}
