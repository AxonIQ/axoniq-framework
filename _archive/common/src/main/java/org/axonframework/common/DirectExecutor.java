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

package org.axonframework.common;

import java.util.concurrent.Executor;

/**
 * Simple executor implementation that runs a given Runnable immediately in the calling thread.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public final class DirectExecutor implements Executor {

    private DirectExecutor() {
    }

    /**
     * Returns a singleton instance of the DirectExecutor. Using this constant prevents the creation of unnecessary
     * DirectExecutor instances.
     */
    public static final DirectExecutor INSTANCE = new DirectExecutor();

    /**
     * Returns the (singleton) instance of the DirectExecutor
     *
     * @return the one and only DirectExecutor
     */
    public static DirectExecutor instance() {
        return INSTANCE;
    }

    /**
     * Executes the given {@code command} immediately in the current thread.
     *
     * @param command the command to execute.
     */
    @Override
    public void execute(Runnable command) {
        command.run();
    }
}
