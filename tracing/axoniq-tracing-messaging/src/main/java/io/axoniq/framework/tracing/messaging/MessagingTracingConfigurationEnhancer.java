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

import io.axoniq.framework.tracing.NoOpSpanFactory;
import io.axoniq.framework.tracing.SpanFactory;
import io.axoniq.framework.tracing.messaging.internal.TracingCommandBus;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.commandhandling.CommandBus;

/**
 * {@link ConfigurationEnhancer} that wires tracing into {@code axon-messaging} components. Discovered automatically via
 * ServiceLoader, so dropping the {@code axoniq-tracing-messaging} module on the classpath is enough to enable messaging
 * tracing.
 * <p>
 * A component is only decorated when a non-no-op {@link SpanFactory} is configured (so tracing imposes no overhead when
 * disabled) and the corresponding toggle in {@link MessagingTracingSettings} is enabled. In this slice the
 * {@link CommandBus} is decorated; further messaging components are added by later slices.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@Internal
public final class MessagingTracingConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * Decorator order for the messaging tracing decorators.
     */
    public static final int TRACING_DECORATOR_ORDER = 0;

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerDecorator(
                CommandBus.class,
                TRACING_DECORATOR_ORDER,
                (config, name, delegate) -> {
                    SpanFactory spanFactory = config.getOptionalComponent(SpanFactory.class)
                                                    .orElse(NoOpSpanFactory.INSTANCE);
                    if (spanFactory instanceof NoOpSpanFactory) {
                        return delegate;
                    }
                    MessagingTracingSettings settings =
                            config.getOptionalComponent(MessagingTracingSettings.class)
                                  .orElseGet(MessagingTracingSettings::enabledByDefault);
                    if (!settings.commandBusEnabled()) {
                        return delegate;
                    }
                    return new TracingCommandBus(delegate, spanFactory);
                }
        );
    }
}
