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

package io.axoniq.framework.messaging.commandhandling.distributed;

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;

import static org.axonframework.common.configuration.DecoratorDefinition.forType;

/**
 * Configuration enhancer that, when a {@link CommandBusConnector} is present in the configuration, may decorate it with
 * a {@link LocalShortcutCommandBusConnector} wrapping a registered {@link LocalCommandDispatchPredicate}.
 * <p>
 * The decorator is registered at {@link #LOCAL_SHORTCUT_CONNECTOR_ORDER}, placing it near the outer edge of the
 * connector decorator chain. As decorators are applied in ascending order and a higher order wraps a lower one, this
 * puts the local shortcut <em>outside</em> every regular connector decorator - most notably the payload-converting
 * connector. Being outermost is essential: when the shortcut takes the local path it never invokes the wrapped
 * connector, so any decorator concerned with the command actually leaving this node (payload (de)serialization,
 * outbound metrics or tracing, routing) sits inside the shortcut and is correctly bypassed. It also means the shortcut
 * operates on unconverted, in-memory commands and captures the unconverted local {@link CommandBusConnector.Handler},
 * skipping payload (de)serialization entirely.
 * <p>
 * The {@link LocalCommandDispatchPredicate} is resolved lazily, at decoration time, rather than when this enhancer
 * runs: this way, a predicate registered by another component after this enhancer still takes effect. When none is
 * registered, the connector is left undecorated, leaving the default distributed dispatch behavior fully intact.
 *
 * @author Allard Buijze
 * @see LocalShortcutCommandBusConnector
 * @see LocalCommandDispatchPredicate
 * @since 5.4.0
 */
@Internal
public class LocalShortcutCommandBusConnectorConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * The order at which the {@link LocalShortcutCommandBusConnector} decorates the {@link CommandBusConnector}.
     * <p>
     * The shortcut short-circuits the connector: on the local path the wrapped connector - and every decorator between
     * this one and it - is never invoked. It must therefore wrap as far <em>outside</em> as practical, so that any
     * decorator acting on a command <em>because</em> it is about to leave this node is bypassed when the command is
     * handled locally. Since decorators are applied in ascending order and a higher order wraps a lower one, a value
     * near the top of the range achieves this. {@link Integer#MAX_VALUE} is deliberately halved rather than used
     * directly: it leaves ample room above for a decorator that must legitimately observe or transform <em>every</em>
     * dispatch, local or remote, by choosing a still-higher order.
     */
    public static final int LOCAL_SHORTCUT_CONNECTOR_ORDER = Integer.MAX_VALUE >> 1;

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        if (componentRegistry.hasComponent(CommandBusConnector.class)) {
            componentRegistry.registerDecorator(
                    forType(CommandBusConnector.class)
                            .with((config, name, delegate) -> decorate(config, delegate))
                            .order(LOCAL_SHORTCUT_CONNECTOR_ORDER)
            );
        }
    }

    private static CommandBusConnector decorate(Configuration config, CommandBusConnector delegate) {
        return config.getOptionalComponent(LocalCommandDispatchPredicate.class)
                     .<CommandBusConnector>map(predicate -> new LocalShortcutCommandBusConnector(delegate, predicate))
                     .orElse(delegate);
    }
}
