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
 * Represents a handler for a component's lifecycle.
 * <p>
 * Unlike the {@link LifecycleHandler}, this handler provides access to the component when it is created.
 * <p>
 * With decorated components, this may not be the same component instance as would be retrieved from the configuration
 * using the component's identifier. The latter would return the decorated component, while the
 * {@code ComponentLifecycleHandler} receives the instance on the level it was registered. If this is a decorator, you
 * get the decorated instance of that registration, not the completed component.
 *
 * @param <C> The type of component.
 * @author Allard Buijze
 * @since 5.0.0
 */
@FunctionalInterface
public interface ComponentLifecycleHandler<C> {

    /**
     * Runs the lifecycle handler for given {@code component} that has been defined within the scope of given
     * {@code configuration}. The configuration may be used to retrieve components that the given component must
     * interact with during its lifecycle.
     * <p>
     * Lifecycle methods may be executed asynchronously. In that case, the returned {@link CompletableFuture} must be
     * completed when the lifecycle operation has completed. Failures may be propagated by completing the
     * {@link CompletableFuture} exceptionally.
     *
     * @param configuration The configuration in which the component was defined.
     * @param component     The instance of the component.
     * @return A future that completes when the lifecycle operation has terminated.
     */
    CompletableFuture<?> run(Configuration configuration, C component);
}
