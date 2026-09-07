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
package io.axoniq.framework.workflow.runtime.test.fakes;


import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Deterministic {@link java.util.concurrent.ExecutorService} fake that runs every submitted task synchronously on the
 * calling thread.
 * <p>
 * The engine's body executor is already an injectable {@code ExecutorService} component
 * ({@code WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR}). Replacing it with this same-thread executor removes
 * thread-scheduling nondeterminism so a simulator drives execution on a single, known thread.
 * <p>
 * This executor cannot be shut down in a way that rejects tasks ({@link #shutdown()} and {@link #shutdownNow()} are
 * no-ops that leave it usable); it is intended purely for tests and simulation where lifecycle management is handled by
 * the harness.
 * <p>
 * Register it via the configurer:
 * {@code componentRegistry(cr -> cr.registerComponent(ExecutorService.class, WORKFLOW_ENGINE_EXECUTOR, cfg -> exec))}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class SameThreadExecutorService extends AbstractExecutorService {

    private volatile boolean shutdown = false;

    @Override
    public void execute(Runnable command) {
        command.run();
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

        @Override
    public List<Runnable> shutdownNow() {
        shutdown = true;
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return true;
    }
}
