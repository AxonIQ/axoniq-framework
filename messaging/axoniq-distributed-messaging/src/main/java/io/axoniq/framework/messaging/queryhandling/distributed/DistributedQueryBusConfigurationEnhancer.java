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
import org.axonframework.common.configuration.ComponentDecorator;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.interception.InterceptingQueryBus;

import static org.axonframework.common.configuration.DecoratorDefinition.forType;

/**
 * Configuration enhancer for the {@link DistributedQueryBus}, which upon detection of a {@link QueryBusConnector} in
 * the configuration will decorate the regular {@link QueryBus} with the provided
 * connector.
 *
 * @author Mateusz Nowak
 * @since 5.0.0
 */
@Internal
public class DistributedQueryBusConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * The order in which the {@link DistributedQueryBus} is applied to the {@link QueryBus} in the
     * {@link ComponentRegistry}. As such, any decorator with a lower value will be applied to the delegate, and any
     * higher value will be applied to the {@link DistributedQueryBus} itself. Using the same value can either lead to
     * application of the decorator to the delegate or the distributed query bus, depending on the order of
     * registration.
     */
    public static final int DISTRIBUTED_QUERY_BUS_ORDER = InterceptingQueryBus.DECORATION_ORDER - 50;

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        if (componentRegistry.hasComponent(QueryBusConnector.class)) {
            componentRegistry
                    .registerIfNotPresent(
                            DistributedQueryBusConfiguration.class,
                            c -> DistributedQueryBusConfiguration.DEFAULT,
                            SearchScope.ALL
                    )
                    .registerDecorator(forType(QueryBus.class).with(queryBusDecoratorDefinition())
                                                              .order(DISTRIBUTED_QUERY_BUS_ORDER));
        }
    }

    private ComponentDecorator<QueryBus, QueryBus> queryBusDecoratorDefinition() {
        return (config, name, delegate) -> {
            if (delegate instanceof DistributedQueryBus) {
                return delegate;
            }
            var queryBusConfiguration = config.getComponent(DistributedQueryBusConfiguration.class);
            return config.getOptionalComponent(QueryBusConnector.class)
                         .map(connector -> distributedQueryBus(delegate, connector, queryBusConfiguration)
                         )
                         .orElse(delegate);
        };
    }

    static QueryBus distributedQueryBus(QueryBus delegate, QueryBusConnector connector,
                                                DistributedQueryBusConfiguration queryBusConfiguration) {
        return new DistributedQueryBus(delegate, connector, queryBusConfiguration);
    }
}
