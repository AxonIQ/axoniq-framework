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

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.jspecify.annotations.Nullable;

/**
 * A {@link ConfigurationEnhancer} that when a {@link QueryBusConnector} is present in the configuration may decorate it
 * with a {@link LocalShortcutQueryBusConnector} wrapping a registered {@link LocalQueryDispatchPredicate}.
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
     * Note that the {@code LocalShortcutQueryBusConnector} shortcut short-circuits the connector. On the local path the
     * wrapped connector, and every decorator between this one and it, is <b>never</b> invoked. It must therefore wrap
     * as far <b>outside</b> as practical, so that any decorator acting on a query <b>because</b> it is about to leave
     * this node is bypassed when the query is handled locally.
     * <p>
     * Since decorators are applied in ascending order and a higher order wraps a lower one, a value near the top of the
     * range achieves this. {@link Integer#MAX_VALUE} is deliberately halved rather than used directly to leave room for
     * other optional decorators.
     */
    public static final int LOCAL_SHORTCUT_CONNECTOR_ORDER = Integer.MAX_VALUE >> 1;

    private static final LocalQueryDispatchPredicate ALWAYS_LOCAL = (query, context) -> true;

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        if (!componentRegistry.hasComponent(QueryBusConnector.class)) {
            return;
        }
        componentRegistry.registerDecorator(
                QueryBusConnector.class,
                LOCAL_SHORTCUT_CONNECTOR_ORDER,
                (config, name, delegate) -> {
                    LocalQueryDispatchPredicate predicate = effectivePredicate(config);
                    return predicate == null
                            ? delegate
                            : new LocalShortcutQueryBusConnector(delegate, predicate);
                }
        );
    }

    /**
     * Resolves the predicate to install, or {@code null} when no shortcut should be applied. A user-registered
     * {@link LocalQueryDispatchPredicate} takes precedence, falling back to an "always local" predicate when the
     * deprecated {@link DistributedQueryBusConfiguration#preferLocalQueryHandler()} setting is enabled.
     */
    private static @Nullable LocalQueryDispatchPredicate effectivePredicate(Configuration config) {
        return config.getOptionalComponent(LocalQueryDispatchPredicate.class)
                     .orElseGet(() -> preferLocalQueryHandler(config) ? ALWAYS_LOCAL : null);
    }

    @SuppressWarnings("removal")
    private static boolean preferLocalQueryHandler(Configuration config) {
        return config.getOptionalComponent(DistributedQueryBusConfiguration.class)
                     .orElse(DistributedQueryBusConfiguration.DEFAULT)
                     .preferLocalQueryHandler();
    }
}
