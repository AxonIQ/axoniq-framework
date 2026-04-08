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

package org.axonframework.common.configuration;

import java.util.concurrent.CompletableFuture;

/**
 * Functional interface towards a lifecycle handler used during start up or shutdown of an application.
 *
 * @author Steven van Beelen
 * @since 4.3.0
 */
@FunctionalInterface
public interface LifecycleHandler {

    /**
     * Run the start-up or shutdown process this {@code LifecycleHandler} represents. Depending on the implementation
     * this might be asynchronous through the return value.
     *
     * @param configuration The configuration that provides access to the components in this application.
     * @return a {@link CompletableFuture} of unknown type which enables chaining several {@code LifecycleHandler}
     * calls.
     */
    CompletableFuture<?> run(Configuration configuration);
}
