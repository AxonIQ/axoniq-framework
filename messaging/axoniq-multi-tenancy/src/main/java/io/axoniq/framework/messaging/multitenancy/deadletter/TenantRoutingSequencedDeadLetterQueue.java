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

package io.axoniq.framework.messaging.multitenancy.deadletter;

import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.EnqueueDecision;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

/**
 * Routes dead-letter queue operations to a queue dedicated to the tenant carried by the processing context.
 * <p>
 * All operations require a processing context carrying a known tenant. An operation without one is rejected, rather
 * than selecting a tenant arbitrarily.
 *
 * @author Jan Galinski
 * @since 5.4.0
 */
@Internal
public class TenantRoutingSequencedDeadLetterQueue implements SequencedDeadLetterQueue<EventMessage> {

    private final String processingGroup;
    private final Configuration configuration;
    private final SequencedDeadLetterQueueFactory factory;
    private final TenantProvider tenantProvider;
    private final Map<TenantDescriptor, SequencedDeadLetterQueue<EventMessage>> queues = new ConcurrentHashMap<>();

    TenantRoutingSequencedDeadLetterQueue(String processingGroup,
                                          Configuration configuration,
                                          SequencedDeadLetterQueueFactory factory,
                                          TenantProvider tenantProvider) {
        this.processingGroup = requireNonNull(processingGroup, "The processing group must not be null");
        this.configuration = requireNonNull(configuration, "The configuration must not be null");
        this.factory = requireNonNull(factory, "The factory must not be null");
        this.tenantProvider = requireNonNull(tenantProvider, "The tenant provider must not be null");
    }

    @Override
    public CompletableFuture<Void> enqueue(Object sequenceIdentifier,
                                           DeadLetter<? extends EventMessage> letter,
                                           @Nullable ProcessingContext context) {
        return queueFor(context).thenCompose(queue -> queue.enqueue(sequenceIdentifier, letter, context));
    }

    @Override
    public CompletableFuture<Void> evict(DeadLetter<? extends EventMessage> letter,
                                         @Nullable ProcessingContext context) {
        return queueFor(context).thenCompose(queue -> queue.evict(letter, context));
    }

    @Override
    public CompletableFuture<Void> requeue(DeadLetter<? extends EventMessage> letter,
                                           UnaryOperator<DeadLetter<? extends EventMessage>> letterUpdater,
                                           @Nullable ProcessingContext context) {
        return queueFor(context).thenCompose(queue -> queue.requeue(letter, letterUpdater, context));
    }

    @Override
    public CompletableFuture<Boolean> contains(Object sequenceIdentifier, @Nullable ProcessingContext context) {
        return queueFor(context).thenCompose(queue -> queue.contains(sequenceIdentifier, context));
    }

    @Override
    public CompletableFuture<Iterable<DeadLetter<? extends EventMessage>>> deadLetterSequence(
            Object sequenceIdentifier, @Nullable ProcessingContext context) {
        return queueFor(context).thenCompose(queue -> queue.deadLetterSequence(sequenceIdentifier, context));
    }

    @Override
    public CompletableFuture<Iterable<Iterable<DeadLetter<? extends EventMessage>>>> deadLetters(
            @Nullable ProcessingContext context) {
        return queueFor(context).thenCompose(queue -> queue.deadLetters(context));
    }

    @Override
    public CompletableFuture<Boolean> isFull(Object sequenceIdentifier, @Nullable ProcessingContext context) {
        return queueFor(context).thenCompose(queue -> queue.isFull(sequenceIdentifier, context));
    }

    @Override
    public CompletableFuture<Long> size(@Nullable ProcessingContext context) {
        return queueFor(context).thenCompose(queue -> queue.size(context));
    }

    @Override
    public CompletableFuture<Long> sequenceSize(Object sequenceIdentifier, @Nullable ProcessingContext context) {
        return queueFor(context).thenCompose(queue -> queue.sequenceSize(sequenceIdentifier, context));
    }

    @Override
    public CompletableFuture<Long> amountOfSequences(@Nullable ProcessingContext context) {
        return queueFor(context).thenCompose(queue -> queue.amountOfSequences(context));
    }

    @Override
    public CompletableFuture<Boolean> process(Predicate<DeadLetter<? extends EventMessage>> sequenceFilter,
                                              Function<DeadLetter<? extends EventMessage>, CompletableFuture<EnqueueDecision<EventMessage>>> processingTask,
                                              @Nullable ProcessingContext context) {
        return queueFor(context).thenCompose(queue -> queue.process(sequenceFilter, processingTask, context));
    }

    @Override
    public CompletableFuture<Void> clear(@Nullable ProcessingContext context) {
        return queueFor(context).thenCompose(queue -> queue.clear(context));
    }

    private CompletableFuture<SequencedDeadLetterQueue<EventMessage>> queueFor(@Nullable ProcessingContext context) {
        TenantDescriptor tenant = context == null ? null : context.getResource(TenantDescriptor.RESOURCE_KEY);

        if (tenant == null || !tenantProvider.isKnown(tenant)) {
            return CompletableFuture.failedFuture(new TenantNotResolvedException(
                    "Dead-letter queue operations require a tenant-carrying processing context"));
        }
        return CompletableFuture.completedFuture(
                queues.computeIfAbsent(tenant, ignored -> factory.create(processingGroup, configuration))
        );
    }
}
