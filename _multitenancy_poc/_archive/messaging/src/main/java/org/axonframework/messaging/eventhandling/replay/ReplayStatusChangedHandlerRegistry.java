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

package org.axonframework.messaging.eventhandling.replay;

import org.axonframework.common.annotation.Internal;

/**
 * Registry for subscribing {@link ReplayStatusChangedHandler} instances.
 * <p>
 * Components implementing this interface accept replay status changed handler subscriptions, allowing dynamic
 * registration of replay status change behavior following the same pattern as event handler registration.
 * <p>
 * Example usage:
 * <pre>{@code
 * ReplayStatusChangedHandlerRegistry registry = ...;
 * registry.subscribe((statusChange, context) -> {
 *     if (statusChange.status() == ReplayStatus.REPLAY) {
 *         repository.deleteAll();
 *     }
 *     return MessageStream.empty();
 * });
 * }</pre>
 *
 * @param <S> the type of the registry itself, used for fluent interfacing
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @see ReplayStatusChangedHandler
 * @see org.axonframework.messaging.eventhandling.EventHandlingComponent
 * @since 5.1.0
 */
@Internal
public interface ReplayStatusChangedHandlerRegistry<S extends ReplayStatusChangedHandlerRegistry<S>> {

    /**
     * Subscribes a replay status changed handler to this registry.
     * <p>
     * The handler will be invoked when the {@link ReplayStatus} changed. Multiple handlers can be subscribed, and all
     * will be invoked when the replay status changes.
     *
     * @param replayStatusChangedHandler the replay status changed handler to subscribe, must not be {@code null}
     * @return this registry instance for method chaining
     */
    S subscribe(ReplayStatusChangedHandler replayStatusChangedHandler);
}
