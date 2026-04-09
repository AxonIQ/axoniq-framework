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

package io.axoniq.framework.messaging.commandhandling.distributed;

import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.interception.InterceptingCommandBus;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentDecorator;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;

import static org.axonframework.common.configuration.DecoratorDefinition.forType;

/**
 * Configuration enhancer for the {@link DistributedCommandBus}, which upon detection of a {@link CommandBusConnector}
 * in the configuration will decorate the regular {@link CommandBus} with the provided
 * connector.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
@Internal
public class DistributedCommandBusConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * The order in which the {@link DistributedCommandBus} is applied to the {@link CommandBus} in the
     * {@link ComponentRegistry}. As such, any decorator with a lower value will be applied to the delegate, and any
     * higher value will be applied to the {@link DistributedCommandBus} itself. Using the same value can either lead to
     * application of the decorator to the delegate or the distributed command bus, depending on the order of
     * registration.
     */
    public static final int DISTRIBUTED_COMMAND_BUS_ORDER = InterceptingCommandBus.DECORATION_ORDER - 50;

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        if (componentRegistry.hasComponent(CommandBusConnector.class)) {
            componentRegistry
                    .registerIfNotPresent(
                            DistributedCommandBusConfiguration.class,
                            (c) -> DistributedCommandBusConfiguration.DEFAULT,
                            SearchScope.ALL
                    )
                    .registerDecorator(forType(CommandBus.class).with(commandBusDecoratorDefinition())
                                                                .order(DISTRIBUTED_COMMAND_BUS_ORDER));
        }
    }

    private ComponentDecorator<CommandBus, CommandBus> commandBusDecoratorDefinition() {
        return (config, name, delegate) -> {
            if (delegate instanceof DistributedCommandBus) {
                return delegate;
            }
            var commandBusConfiguration = config.getComponent(DistributedCommandBusConfiguration.class);
            return config.getOptionalComponent(CommandBusConnector.class)
                         .map(connector -> (CommandBus) new DistributedCommandBus(
                                 delegate, connector, commandBusConfiguration
                         ))
                         .orElse(delegate);
        };
    }
}