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

package io.axoniq.framework.messaging.distributed.tracing;

import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.configuration.TracingConfigurationOrder;
import io.axoniq.framework.messaging.commandhandling.distributed.tracing.TracingCommandBusConnector;
import io.axoniq.framework.messaging.queryhandling.distributed.tracing.TracingQueryBusConnector;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.jspecify.annotations.Nullable;

/**
 * {@link ConfigurationEnhancer} that wires tracing into the distributed bus connectors. Discovered automatically via
 * ServiceLoader, so having the {@code axoniq-distributed-messaging} module on the classpath is enough to enable
 * connector tracing.
 * <p>
 * A connector is only decorated when a non-no-op {@link SpanFactory} is configured and the corresponding toggle in
 * {@link DistributedTracingSettings} is enabled.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@Internal
@RegistrationScope("Register decorators once at the root; do not re-invoke in child module registries "
        + "(the DecoratorDefinitions are copied down and reach module-built components on their own). "
        + "Re-invoking per nesting level re-registers the decorators and produces duplicate nested spans.")
public final class DistributedTracingConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * Decorator order for the distributed-tracing decorators. Near-maximal so tracing is applied last and is the
     * <em>outermost</em> wrapper — spans cover all inner decorators, and tracing wrappers are reliably detectable by
     * an outermost {@code instanceof} check. Same value and rationale as
     * {@code MessagingTracingConfigurationEnhancer#TRACING_DECORATOR_ORDER}.
     */
    public static final int TRACING_DECORATOR_ORDER = TracingConfigurationOrder.TRACING_DECORATOR_ORDER;

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerIfNotPresent(DistributedTracingSettings.class,
                                      c -> DistributedTracingSettings.enabledByDefault());
        registry.registerDecorator(
                CommandBusConnector.class,
                TRACING_DECORATOR_ORDER,
                (config, name, delegate) -> {
                    SpanFactory spanFactory = spanFactory(config);
                    if (spanFactory == null || !settings(config).commandBusConnectorEnabled()) {
                        return delegate;
                    }
                    return new TracingCommandBusConnector(delegate, spanFactory);
                }
        );
        registry.registerDecorator(
                QueryBusConnector.class,
                TRACING_DECORATOR_ORDER,
                (config, name, delegate) -> {
                    SpanFactory spanFactory = spanFactory(config);
                    if (spanFactory == null || !settings(config).queryBusConnectorEnabled()) {
                        return delegate;
                    }
                    return new TracingQueryBusConnector(delegate, spanFactory);
                }
        );
    }

    /**
     * Resolves the configured {@link SpanFactory}, or {@code null} when none is configured (tracing disabled). The
     * {@code SpanFactory} is an optional bean; when absent {@link Configuration#getComponent(Class)} throws
     * {@link ComponentNotFoundException}, translated to {@code null} so the connector is left undecorated.
     */
    private static @Nullable SpanFactory spanFactory(Configuration config) {
        try {
            return config.getComponent(SpanFactory.class);
        } catch (ComponentNotFoundException e) {
            return null;
        } catch (RuntimeException e) {
            // A Spring-backed Configuration signals absence with its own exception type (NoSuchBeanDefinitionException
            // from SpringComponentRegistry's getComponent(Class)) instead of ComponentNotFoundException. Distinguish
            // absence from a genuine construction failure via the optional lookup: absent -> undecorated; a factory
            // that fails to construct rethrows from the optional lookup and still surfaces.
            return config.getOptionalComponent(SpanFactory.class).orElse(null);
        }
    }

    /**
     * Resolves the {@link DistributedTracingSettings} component. Always present: registered as a default by
     * {@link #enhance(ComponentRegistry)} via {@code registerIfNotPresent}, unless a user-supplied or
     * property-translated registration took precedence.
     */
    private static DistributedTracingSettings settings(Configuration config) {
        return config.getComponent(DistributedTracingSettings.class);
    }

    @Override
    public int order() {
        return TracingConfigurationOrder.TRACING_DEFAULTS_ENHANCER_ORDER;
    }
}
