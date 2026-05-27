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

package io.axoniq.framework.tracing.messaging;

import org.axonframework.common.annotation.Internal;

/**
 * Per-component on/off toggles for messaging tracing, read by {@code MessagingTracingConfigurationEnhancer} to decide
 * which {@code axon-messaging} components to decorate.
 * <p>
 * This is registered as a framework component by the Spring autoconfiguration (populated from
 * {@code axon.tracing.*} properties). When absent from the configuration, every component defaults to enabled. It is
 * {@code @Internal} because it is the integration point between the Spring property model and the ServiceLoader-
 * discovered enhancer, not a type applications construct directly.
 *
 * @param commandBusEnabled whether the {@code CommandBus} is decorated with tracing
 * @author Mateusz Nowak
 * @since 5.2.0
 */
@Internal
public record MessagingTracingSettings(boolean commandBusEnabled) {

    /**
     * Returns the default settings, with every messaging component enabled for tracing.
     *
     * @return the all-enabled default settings
     */
    public static MessagingTracingSettings enabledByDefault() {
        return new MessagingTracingSettings(true);
    }
}
