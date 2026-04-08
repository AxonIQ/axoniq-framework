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

package org.axonframework.messaging.monitoring.configuration;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.monitoring.MessageMonitor;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Functional interface for building a {@link MessageMonitor} for a specific component type and component name.
 * <p>
 * This interface allows {@code MessageMonitors} to be constructed with knowledge of the component they will monitor,
 * allowing for fine-grained control on how or when to construct an interceptor. This, for example, enables proper
 * tagging and categorization in metric systems.
 *
 * @param <M> the type of {@link Message} the resulting {@link MessageMonitor} will monitor
 * @author Steven van Beelen
 * @since 5.0.3
 */
@FunctionalInterface
public interface MessageMonitorFactory<M extends Message> {

    /**
     * Builds a {@link MessageMonitor} for the specified component.
     *
     * @param config        the {@link Configuration} from which other components can be retrieved during construction
     * @param componentType the type of the component to build a monitor for
     * @param componentName the name of the component to build a monitor for
     * @return a {@link MessageMonitor} instance configured for the specified component or {@code null} when no monitor
     * is required for the given {@code componentType} and {@code componentName} combination
     */
    @Nullable
    MessageMonitor<? super M> build(
            @NonNull Configuration config,
            @NonNull Class<?> componentType,
            @Nullable String componentName
    );
}