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
import org.axonframework.common.configuration.ConfigurationEnhancer;

/**
 * A {@link ConfigurationEnhancer} that when a {@link CommandBusConnector} is present in the configuration may decorate
 * it with a {@link LocalShortcutCommandBusConnector} wrapping a registered {@link LocalCommandDispatchPredicate}.
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
     * Note that the {@code LocalShortcutCommandBusConnector} shortcut short-circuits the connector. On the local path
     * the wrapped connector, and every decorator between this one and it, is <b>never</b> invoked. It must therefore
     * wrap as far <b>outside</b> as practical, so that any decorator acting on a command <b>because</b> it is about to
     * leave this node is bypassed when the command is handled locally.
     * <p>
     * Since decorators are applied in ascending order and a higher order wraps a lower one, a value near the top of the
     * range achieves this. {@link Integer#MAX_VALUE} is deliberately halved rather than used directly to leave room for
     * other optional decorators.
     */
    public static final int LOCAL_SHORTCUT_CONNECTOR_ORDER = Integer.MAX_VALUE >> 1;

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        if (!componentRegistry.hasComponent(CommandBusConnector.class)) {
            return;
        }

        componentRegistry.registerDecorator(
                CommandBusConnector.class,
                LOCAL_SHORTCUT_CONNECTOR_ORDER,
                (config, name, delegate) -> config.getOptionalComponent(LocalCommandDispatchPredicate.class)
                                                  .<CommandBusConnector>map(predicate -> new LocalShortcutCommandBusConnector(
                                                          delegate, predicate
                                                  ))
                                                  .orElse(delegate)
        );
    }
}
