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

package org.axonframework.extension.spring.config;

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.LifecycleHandler;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * A {@link SmartLifecycle} implementation wrapping a
 * {@link LifecycleHandler start-specific lifecycle handler} to allow it to be managed
 * by Spring.
 *
 * @author Allard Buijze
 * @since 5.0.0
 */
@Internal
public class SpringLifecycleStartHandler implements SmartLifecycle {

    private final int phase;
    private final Supplier<CompletableFuture<?>> task;

    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Initialize the bean to have the given {@code task} executed on start-up in the given {@code phase}.
     *
     * @param phase The start-up phase to invoke the task in.
     * @param task  The task to execute on start-up.
     */
    SpringLifecycleStartHandler(int phase,
                                Supplier<CompletableFuture<?>> task) {
        this.phase = phase;
        this.task = task;
    }

    @Override
    public void start() {
        try {
            task.get()
                .whenComplete((result, throwable) -> running.set(true))
                .get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        } catch (ExecutionException e) {
            // This is what the join() would throw
            throw new CompletionException(e);
        }
    }

    @Override
    public void stop() {
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return phase;
    }
}
