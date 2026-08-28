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

package io.axoniq.framework.messaging.queryhandling.distributed;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;

import static org.axonframework.common.configuration.DecoratorDefinition.forType;

/**
 * Configuration enhancer that, when a {@link QueryBusConnector} is present in the configuration, may decorate it with a
 * {@link LocalShortcutQueryBusConnector}.
 * <p>
 * Whether the shortcut is installed, and with which {@link LocalQueryDispatchPredicate}, is decided per configuration:
 * <ul>
 *     <li>when a {@link LocalQueryDispatchPredicate} is registered as a component, that predicate is used;</li>
 *     <li>otherwise the deprecated {@link DistributedQueryBusConfiguration#preferLocalQueryHandler()} setting is
 *     honored: when {@code true} (the default) an "always local" predicate is used, when {@code false} no shortcut is
 *     installed and the connector is left untouched.</li>
 * </ul>
 * This keeps the {@code preferLocalQueryHandler} behavior working while routing it through the same connector-level
 * shortcut used for the {@link LocalQueryDispatchPredicate}, so there is a single shortcut mechanism.
 * <p>
 * The decorator is registered at {@link #LOCAL_SHORTCUT_CONNECTOR_ORDER}, placing it near the outer edge of the
 * connector decorator chain. Being outermost is essential: when the shortcut takes the local path it never invokes the
 * wrapped connector, so any decorator concerned with the query actually leaving this node (payload (de)serialization,
 * outbound metrics or tracing, routing) sits inside the shortcut and is correctly bypassed. It also means the shortcut
 * operates on unconverted, in-memory queries and captures the unconverted local {@link QueryBusConnector.Handler},
 * skipping payload (de)serialization entirely.
 *
 * @author Allard Buijze
 * @see LocalShortcutQueryBusConnector
 * @see LocalQueryDispatchPredicate
 * @since 5.4.0
 */
@Internal
public class LocalShortcutQueryBusConnectorConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * The order at which the {@link LocalShortcutQueryBusConnector} decorates the {@link QueryBusConnector}.
     * <p>
     * The shortcut short-circuits the connector: on the local path the wrapped connector - and every decorator between
     * this one and it - is never invoked. It must therefore wrap as far <em>outside</em> as practical, so that any
     * decorator acting on a query <em>because</em> it is about to leave this node is bypassed when the query is handled
     * locally. Since decorators are applied in ascending order and a higher order wraps a lower one, a value near the
     * top of the range achieves this. {@link Integer#MAX_VALUE} is deliberately halved rather than used directly: it
     * leaves ample room above for a decorator that must legitimately observe or transform <em>every</em> query, local
     * or remote, by choosing a still-higher order.
     *
     * @since 5.3.0
     */
    public static final int LOCAL_SHORTCUT_CONNECTOR_ORDER = Integer.MAX_VALUE >> 1;

    private static final LocalQueryDispatchPredicate ALWAYS_LOCAL = (query, context) -> true;

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        if (componentRegistry.hasComponent(QueryBusConnector.class)) {
            componentRegistry.registerDecorator(
                    forType(QueryBusConnector.class)
                            .with((config, name, delegate) -> {
                                LocalQueryDispatchPredicate predicate = effectivePredicate(config);
                                return predicate == null
                                        ? delegate
                                        : new LocalShortcutQueryBusConnector(delegate, predicate);
                            })
                            .order(LOCAL_SHORTCUT_CONNECTOR_ORDER)
            );
        }
    }

    /**
     * Resolves the predicate to install, or {@code null} when no shortcut should be applied: a user-registered
     * {@link LocalQueryDispatchPredicate} takes precedence, falling back to an "always local" predicate when the
     * deprecated {@link DistributedQueryBusConfiguration#preferLocalQueryHandler()} setting is enabled.
     */
    private static @Nullable LocalQueryDispatchPredicate effectivePredicate(Configuration config) {
        return config.getOptionalComponent(LocalQueryDispatchPredicate.class)
                     .orElseGet(() -> preferLocalQueryHandler(config) ? ALWAYS_LOCAL : null);
    }

    private static boolean preferLocalQueryHandler(Configuration config) {
        return config.getOptionalComponent(DistributedQueryBusConfiguration.class)
                     .orElse(DistributedQueryBusConfiguration.DEFAULT)
                     .preferLocalQueryHandler();
    }
}
