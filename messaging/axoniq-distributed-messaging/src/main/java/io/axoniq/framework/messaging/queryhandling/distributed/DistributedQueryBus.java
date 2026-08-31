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

package io.axoniq.framework.messaging.queryhandling.distributed;

import org.axonframework.common.FutureUtils;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.util.PriorityRunnable;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.DelayedMessageStream;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryHandler;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Implementation of a {@code QueryBus} that is aware of multiple instances of a {@code QueryBus} working together to
 * spread the load.
 * <p>
 * Each "physical" {@code QueryBus} instance is considered a "segment" of a conceptual distributed {@code QueryBus}.
 * <p>
 * The {@code DistributedQueryBus} relies on a {@link QueryBusConnector} to dispatch queries and query responses to
 * different segments of the {@code QueryBus}. Depending on the implementation used, each segment may run in a different
 * JVM.
 *
 * @author Jan Galinski
 * @author Steven van Beelen
 * @since 5.0.0
 */
public class DistributedQueryBus implements QueryBus {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Context.ResourceKey<List<Runnable>> UPDATE_TASKS_KEY =
            Context.ResourceKey.withLabel("update-tasks");

    private final QueryBus localSegment;
    private final QueryBusConnector connector;
    private final ExecutorService queryingExecutor;
    private final Map<QueryMessage, QueryBusConnector.UpdateCallback> updateRegistry = new ConcurrentHashMap<>();

    /**
     * Constructs a {@code DistributedQueryBus} using the given {@code localSegment} for
     * {@link #subscribe(QualifiedName, QueryHandler) subscribing} handlers and the given {@code connector} to dispatch
     * and receive queries and query responses with, to and from different segments of the {@code QueryBus}.
     *
     * @param localSegment  the local {@code QueryBus} used to subscribe handlers to
     * @param connector     the {@code QueryBusConnector} to dispatch and receive queries and query responses with
     * @param configuration the {@code DistributedQueryBusConfiguration} containing the {@link ExecutorService} for
     *                      query processing
     */
    public DistributedQueryBus(QueryBus localSegment,
                               QueryBusConnector connector,
                               DistributedQueryBusConfiguration configuration) {
        this.localSegment = localSegment;
        this.connector = connector;
        this.queryingExecutor = configuration.queryExecutorService();
        connector.onIncomingQuery(new DistributedHandler());
    }

    @Override
    public QueryBus subscribe(QualifiedName queryName,
                              QueryHandler queryHandler) {
        localSegment.subscribe(queryName, queryHandler);
        FutureUtils.joinAndUnwrap(connector.subscribe(queryName));
        return this;
    }

    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query,
                                                     @Nullable ProcessingContext context) {
        return connector.query(query, context);
    }

    @Override
    public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                 @Nullable ProcessingContext context,
                                                                 int updateBufferSize) {
        return connector.subscriptionQuery(query, context, updateBufferSize);
    }

    @Override
    public MessageStream<SubscriptionQueryUpdateMessage> subscribeToUpdates(QueryMessage query,
                                                                            int updateBufferSize) {
        // not ideal, but the AxonServer Connector doesn't support just subscribing to update yet
        return subscriptionQuery(query, null, updateBufferSize)
                .filter(e -> e.message() instanceof SubscriptionQueryUpdateMessage)
                .cast();
    }

    @Override
    public CompletableFuture<Void> emitUpdate(Predicate<QueryMessage> filter,
                                              Supplier<SubscriptionQueryUpdateMessage> updateSupplier,
                                              @Nullable ProcessingContext context) {
        return emitUpdateAndCount(filter, updateSupplier, context)
                .thenApply(FutureUtils::ignoreResult);
    }

    @Override
    public CompletableFuture<OptionalInt> emitUpdateAndCount(Predicate<QueryMessage> filter,
                                                             Supplier<SubscriptionQueryUpdateMessage> updateSupplier,
                                                             @Nullable ProcessingContext context) {
        return runAfterCommitOrImmediately(context, filter, () -> emitUpdate(filter, updateSupplier));
    }

    private CompletableFuture<OptionalInt> emitUpdate(Predicate<QueryMessage> filter,
                                                      Supplier<SubscriptionQueryUpdateMessage> updateSupplier) {
        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        updateRegistry.forEach((message, sender) -> {
            if (filter.test((message))) {
                tasks.add(sender.sendUpdate(updateSupplier.get()));
            }
        });
        return CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0]))
                                .thenApply(v -> OptionalInt.of(tasks.size()));
    }

    @Override
    public CompletableFuture<Void> completeSubscriptions(Predicate<QueryMessage> filter,
                                                         @Nullable ProcessingContext context) {
        return completeSubscriptionsAndCount(filter, context)
                .thenApply(FutureUtils::ignoreResult);
    }

    @Override
    public CompletableFuture<OptionalInt> completeSubscriptionsAndCount(Predicate<QueryMessage> filter,
                                                                        @Nullable ProcessingContext context) {
        return runAfterCommitOrImmediately(context, filter, () -> completeSubscriptions(filter));
    }

    private CompletableFuture<OptionalInt> completeSubscriptions(Predicate<QueryMessage> filter) {
        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        updateRegistry.forEach((message, sender) -> {
            if (filter.test((message))) {
                tasks.add(sender.complete());
            }
        });
        return CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0]))
                                .thenApply(v -> OptionalInt.of(tasks.size()));
    }

    @Override
    public CompletableFuture<Void> completeSubscriptionsExceptionally(
            Predicate<QueryMessage> filter,
            Throwable cause,
            @Nullable ProcessingContext context
    ) {
        return completeSubscriptionsExceptionallyAndCount(filter, cause, context)
                .thenApply(FutureUtils::ignoreResult);
    }

    @Override
    public CompletableFuture<OptionalInt> completeSubscriptionsExceptionallyAndCount(
            Predicate<QueryMessage> filter,
            Throwable cause,
            @Nullable ProcessingContext context
    ) {
        return runAfterCommitOrImmediately(context, filter, () -> completeSubscriptionsExceptionally(filter, cause));
    }

    private CompletableFuture<OptionalInt> completeSubscriptionsExceptionally(Predicate<QueryMessage> filter,
                                                                              Throwable cause) {
        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        updateRegistry.forEach((message, sender) -> {
            if (filter.test((message))) {
                tasks.add(sender.completeExceptionally(cause));
            }
        });
        return CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0]))
                                .thenApply(v -> OptionalInt.of(tasks.size()));
    }

    /**
     * Runs the given {@code updateTask} immediately, or defers it until the given {@code context} commits, matching
     * {@code updateTask}'s subscriptions to the given {@code filter}.
     * <p>
     * When run immediately, the returned count reflects the actual outcome of the {@code updateTask} - e.g. the number
     * of subscribers an update was successfully dispatched to, as opposed to the number of subscribers merely matching
     * the {@code filter} before dispatch was attempted.
     * <p>
     * When deferred to after commit, the {@code updateTask} itself runs later and its outcome is not available to this
     * method. In that case, the returned count is a match count against the given {@code filter}, computed at call time
     * since waiting for that outcome would require blocking until after the given {@code context} commits. This could
     * deadlock a caller invoking this from within that same commit pipeline.
     * <p>
     * The match count is only computed when the {@code updateTask} will actually run (now or after commit), not for an
     * already-errored {@code context}, since the update is silently dropped in that case and the {@code filter} must
     * not observe any side effects.
     */
    private CompletableFuture<OptionalInt> runAfterCommitOrImmediately(
            @Nullable ProcessingContext context,
            Predicate<QueryMessage> filter,
            Supplier<CompletableFuture<OptionalInt>> updateTask
    ) {
        if (context == null || context.isCommitted()) {
            return invokeSafely(updateTask);
        } else if (!context.isCompleted()) {
            int matchCount = matchCount(filter);
            context.computeResourceIfAbsent(
                           UPDATE_TASKS_KEY,
                           () -> {
                               List<Runnable> subscriptionQueryTasks = new ArrayList<>();
                               context.runOnAfterCommit(c -> subscriptionQueryTasks.forEach(Runnable::run));
                               return subscriptionQueryTasks;
                           }
                   )
                   .add(() -> invokeSafely(updateTask).whenComplete((result, exception) -> {
                       if (exception != null) {
                           logger.warn("An error occurred while delivering a deferred subscription query update.",
                                       exception);
                       }
                   }));
            return CompletableFuture.completedFuture(OptionalInt.of(matchCount));
        }
        // else: context completed with an error - drop the update
        return CompletableFuture.completedFuture(OptionalInt.empty());
    }

    /**
     * Invokes the given {@code updateTask}, converting any exception it throws synchronously into a failed
     * {@link CompletableFuture} rather than letting it propagate to the caller.
     */
    private static <T> CompletableFuture<T> invokeSafely(Supplier<CompletableFuture<T>> updateTask) {
        try {
            return updateTask.get();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private int matchCount(Predicate<QueryMessage> filter) {
        return (int) updateRegistry.keySet()
                                   .stream()
                                   .filter(filter)
                                   .count();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(localSegment);
        descriptor.describeProperty("connector", connector);
    }

    private class DistributedHandler implements QueryBusConnector.Handler {

        private static final AtomicLong TASK_SEQUENCE = new AtomicLong(Long.MIN_VALUE);

        @Override
        public MessageStream<QueryResponseMessage> query(QueryMessage query) {
            int priority = query.priority().orElse(0);
            if (logger.isDebugEnabled()) {
                logger.debug("Received query [{}] for processing with priority [{}].",
                             query.type(), priority);
            }
            long sequence = TASK_SEQUENCE.incrementAndGet();
            CompletableFuture<MessageStream<QueryResponseMessage>> localResult = new CompletableFuture<>();
            queryingExecutor.execute(
                    new PriorityRunnable(() -> {
                        try {
                            localResult.complete(localSegment.query(query, null));
                        } catch (Exception e) {
                            localResult.completeExceptionally(e);
                        }
                    }, priority, sequence));

            return DelayedMessageStream.create(localResult);
        }

        @Override
        public Registration registerUpdateHandler(QueryMessage subscriptionQueryMessage,
                                                  QueryBusConnector.UpdateCallback updateCallback) {
            updateRegistry.put(subscriptionQueryMessage, updateCallback);
            return () -> updateRegistry.remove(subscriptionQueryMessage, updateCallback);
        }
    }
}
