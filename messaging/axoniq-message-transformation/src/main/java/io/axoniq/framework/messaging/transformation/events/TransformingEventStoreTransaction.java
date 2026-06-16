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

import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.eventsourcing.eventstore.Position;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

/**
 * Wrapping {@link EventStoreTransaction} returned by
 * {@link TransformingEventStore#transaction(org.axonframework.messaging.core.unitofwork.ProcessingContext)}.
 * Applies the chain to {@link #source(SourcingCondition)} only; append / position methods
 * delegate unchanged because the chain runs at read time.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
final class TransformingEventStoreTransaction implements EventStoreTransaction {

    private final EventStoreTransaction delegate;
    private final EventTransformerChain chain;
    private final ProcessingContext context;
    private final MessageConverter converter;
    private final MessageTypeResolver messageTypeResolver;

    /**
     * Package-private; instances are produced by
     * {@link TransformingEventStore#transaction(ProcessingContext)}.
     *
     * @param delegate             the inner {@link EventStoreTransaction} to wrap
     * @param chain                the application's {@link EventTransformerChain}
     * @param context              in the active processing context the wrapped transaction was
     *                             created for; threaded through to mappers via the chain
     * @param converter            the active {@link MessageConverter}
     * @param messageTypeResolver  the active {@link MessageTypeResolver} used to verify
     *                             mapper output identity against the declared {@code to}
     */
    TransformingEventStoreTransaction(EventStoreTransaction delegate,
                                       EventTransformerChain chain,
                                       ProcessingContext context,
                                       MessageConverter converter,
                                       MessageTypeResolver messageTypeResolver) {
        this.delegate = requireNonNull(delegate, "delegate may not be null");
        this.chain = requireNonNull(chain, "chain may not be null");
        this.context = requireNonNull(context, "context may not be null");
        this.converter = requireNonNull(converter, "converter may not be null");
        this.messageTypeResolver = requireNonNull(messageTypeResolver, "messageTypeResolver may not be null");
    }

    @Override
    public MessageStream<? extends EventMessage> source(SourcingCondition condition,
                                                         @Nullable Consumer<Position> resumePositionCallback) {
        return chain.transform(
                delegate.source(condition, resumePositionCallback), context, converter, messageTypeResolver);
    }

    @Override
    public void appendEvent(EventMessage eventMessage) {
        delegate.appendEvent(eventMessage);
    }

    @Override
    public void onAppend(Consumer<EventMessage> callback) {
        delegate.onAppend(callback);
    }

    @Override
    public void overrideAppendCondition(UnaryOperator<AppendCondition> conditionOverride) {
        delegate.overrideAppendCondition(conditionOverride);
    }

    @Override
    public ConsistencyMarker appendPosition() {
        return delegate.appendPosition();
    }
}
