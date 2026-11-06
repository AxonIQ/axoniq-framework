/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.messaging.transformation.events;

import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * A {@link SubscribableEventSource} decorator applying an {@link EventTransformerChain} to every batch of events the
 * delegate hands to its subscriber, so that subscribing event processors see transformed events.
 * <p>
 * Every event of a batch is transformed before the subscriber receives the batch. A transformation that drops an
 * event leaves it out of the batch, and one that splits an event puts all resulting events in its place. When no event
 * remains, the subscriber is not called.
 * <p>
 * Connectors that consume events from outside the application, such as the Kafka connector, wrap their sources with
 * this decorator when an {@link EventTransformerChain} is configured:
 * <pre>{@code
 * SubscribableEventSource transformed =
 *         new TransformingSubscribableEventSource(kafkaSource, chain, messageConverter, messageTypeResolver);
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public final class TransformingSubscribableEventSource implements SubscribableEventSource, DescribableComponent {

    private final SubscribableEventSource delegate;
    private final EventTransformerChain chain;
    private final MessageConverter converter;
    private final MessageTypeResolver messageTypeResolver;
    private final Map<BiFunction<List<? extends EventMessage>, @Nullable ProcessingContext, CompletableFuture<?>>,
            BiFunction<List<? extends EventMessage>, @Nullable ProcessingContext, CompletableFuture<?>>> wrapped =
            new ConcurrentHashMap<>();

    /**
     * Constructs the decorator.
     *
     * @param delegate            the source to subscribe to
     * @param chain               the {@link EventTransformerChain} to apply
     * @param converter           the {@link MessageConverter} used to convert payloads to each transformation's input
     *                            type
     * @param messageTypeResolver the {@link MessageTypeResolver} used to verify each mapper's output identity
     */
    public TransformingSubscribableEventSource(SubscribableEventSource delegate,
                                               EventTransformerChain chain,
                                               MessageConverter converter,
                                               MessageTypeResolver messageTypeResolver) {
        this.delegate = requireNonNull(delegate, "delegate may not be null");
        this.chain = requireNonNull(chain, "chain may not be null");
        this.converter = requireNonNull(converter, "converter may not be null");
        this.messageTypeResolver = requireNonNull(messageTypeResolver, "messageTypeResolver may not be null");
    }

    @Override
    public Registration subscribe(
            BiFunction<List<? extends EventMessage>, @Nullable ProcessingContext, CompletableFuture<?>>
                    eventsBatchConsumer
    ) {
        requireNonNull(eventsBatchConsumer, "eventsBatchConsumer may not be null");
        // The same subscriber is always wrapped by the same function, so that the delegate recognizes a repeated
        // subscription of the same subscriber.
        var transforming = wrapped.computeIfAbsent(
                eventsBatchConsumer,
                consumer -> (events, context) -> transform(events, context).thenCompose(
                        transformed -> transformed.isEmpty()
                                ? CompletableFuture.completedFuture(null)
                                : consumer.apply(transformed, context)
                )
        );
        Registration registration = delegate.subscribe(transforming);
        return () -> {
            boolean cancelled = registration.cancel();
            if (cancelled) {
                wrapped.remove(eventsBatchConsumer, transforming);
            }
            return cancelled;
        };
    }

    private CompletableFuture<List<EventMessage>> transform(List<? extends EventMessage> events,
                                                            @Nullable ProcessingContext context) {
        return chain.transform(MessageStream.fromIterable(events), context, converter, messageTypeResolver)
                    .collect(ArrayList::new, List::add);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("delegate", delegate);
        descriptor.describeProperty("chain", chain);
        descriptor.describeProperty("converter", converter);
        descriptor.describeProperty("messageTypeResolver", messageTypeResolver);
    }
}
