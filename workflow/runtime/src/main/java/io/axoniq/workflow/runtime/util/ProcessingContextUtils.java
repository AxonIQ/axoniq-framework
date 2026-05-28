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
package io.axoniq.workflow.runtime.util;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * Processing context utility functions.
 *
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 1.0.0
 */
@Internal
public class ProcessingContextUtils {

    /**
     * Resource key for a workflow execution restart token.
     */
    public static final Context.ResourceKey<Optional<TrackingToken>> RESTART_TOKEN_RESOURCE_KEY =
            Context.ResourceKey.withLabel("workflowRestartToken");


    private ProcessingContextUtils() {
        // avoid instantiation
    }

    /**
     * Executes supplied action in a new processing context created via unit of work factory as a child of the provided
     * processing context.
     *
     * @param id                id of the unit of work.
     * @param unitOfWorkFactory unit of work factory to use.
     * @param executor          executor to offload the action to.
     * @param parentContext     parent processing context.
     * @param action            action to execute.
     * @param <R>               type of action result.
     * @return result of the action encapsulated in completable future.
     */
    public static <R> CompletableFuture<R> executeWithResult(
            @Nullable String id,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull Executor executor,
            @Nonnull Context parentContext,
            @Nonnull Function<ProcessingContext, CompletableFuture<R>> action) {
        var uow = (id == null)
                ? unitOfWorkFactory.create(customize -> customize.workScheduler(executor))
                : unitOfWorkFactory.create(UUID.randomUUID().toString(),
                                           customize -> customize.workScheduler(executor)); // FIXME
        return uow.executeWithResult(c -> {
            var ctx = ProcessingContextUtils.copyResources(parentContext, c);
            ctx.whenComplete(completed ->
                                     LoggerFactory.getLogger(ProcessingContextUtils.class)
                                                  .trace("ProcessingContext {} completed", completed)
            );
            ctx.onAfterCommit(after -> {
                LoggerFactory.getLogger(ProcessingContextUtils.class).trace("ProcessingContext {} after", after);
                return CompletableFuture.completedFuture(null);
            });
            ctx.onPrepareCommit(prepare -> {
                LoggerFactory.getLogger(ProcessingContextUtils.class).trace("ProcessingContext {} prepare", prepare);
                return CompletableFuture.completedFuture(null);
            });

            return action.apply(ctx);
        });
    }

    /**
     * Executes supplied action in a new thread and processing context created via unit of work factory as a child of
     * the provided processing context. See
     * {@link #executeWithResult(String, UnitOfWorkFactory, Executor, Context, Function)} for running the action in the
     * same thread.
     *
     * @param id                id of the unit of work.
     * @param unitOfWorkFactory unit of work factory to use.
     * @param executorService   executor service to offload the action to.
     * @param parentContext     parent processing context.
     * @param action            action to execute.
     * @param <R>               type of action result.
     */
    public static <R> void executeWithResultInSeparateThread(
            @Nullable String id,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull ExecutorService executorService,
            @Nonnull ProcessingContext parentContext,
            @Nonnull Function<ProcessingContext, CompletableFuture<R>> action) {
        executorService.execute(() -> executeWithResult(id,
                                                        unitOfWorkFactory,
                                                        executorService,
                                                        parentContext,
                                                        action).join() // FIXME join without timeout
        );
    }


    /**
     * Copies resources from the given context to the target processing context.
     *
     * @param from source containing resources.
     * @param to   target processing context.
     * @return resulting processing context.
     */
    public static ProcessingContext copyResources(Context from,
                                                  ProcessingContext to) {
        var fromResource = from.resources();
        //noinspection unchecked
        fromResource.forEach((k, v) -> to.putResource((Context.ResourceKey<Object>) k, v));
        return to;
    }

    /**
     * Resolves a workflow execution restart token from the processing context.
     *
     * @param processingContext processing context
     * @return restart token or current tracking token if no restart token is present
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public static TrackingToken resolveRestartToken(@Nonnull ProcessingContext processingContext) {
        var resource = Objects.requireNonNull((Optional<TrackingToken>)
                                                      processingContext.resources().get(RESTART_TOKEN_RESOURCE_KEY),
                                              "Restart token resource not found");
        return resource.orElse((TrackingToken) processingContext.resources().get(TrackingToken.RESOURCE_KEY));
    }
}
