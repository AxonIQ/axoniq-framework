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

package io.axoniq.framework.springcloud;

import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.commandhandling.distributed.PayloadConvertingCommandBusConnector;
import io.axoniq.framework.messaging.queryhandling.distributed.PayloadConvertingQueryBusConnector;
import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import io.axoniq.framework.springcloud.command.IncomingCommandInvoker;
import io.axoniq.framework.springcloud.command.RemoteCommandDispatcher;
import io.axoniq.framework.springcloud.command.SpringCloudCommandBusConnector;
import io.axoniq.framework.springcloud.query.IncomingQueryInvoker;
import io.axoniq.framework.springcloud.query.RemoteQueryDispatcher;
import io.axoniq.framework.springcloud.query.SpringCloudQueryBusConnector;
import io.axoniq.framework.springcloud.shared.SpringCloudMemberRegistry;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.ComponentDecorator;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentLifecycleHandler;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ConfigurationEnhancer}, auto-loadable by the {@link ApplicationConfigurer}, registering the
 * {@link SpringCloudCommandBusConnector} as the {@link CommandBusConnector} to distribute commands with, and the
 * {@link SpringCloudQueryBusConnector} as the {@link QueryBusConnector} to distribute queries with.
 * <p>
 * Registering a connector is all that is needed to distribute messages: the framework's own
 * {@code DistributedCommandBusConfigurationEnhancer} and {@code DistributedQueryBusConfigurationEnhancer} decorate
 * whatever {@code CommandBus} and {@code QueryBus} are configured into their distributed counterparts as soon as a
 * connector is present. No bus needs to be constructed here.
 * <p>
 * This enhancer does nothing unless a {@link SpringCloudMemberRegistry} is present, which the Spring Boot
 * autoconfiguration provides once a Spring Cloud {@code DiscoveryClient} is on hand. The two connectors are registered
 * independently, so a configuration already carrying one of them still gains the other.
 * <p>
 * Distributing messages through Spring Cloud and through Axon Server are alternatives, not layers: an application uses
 * one or the other. This enhancer does not attempt to diagnose an application that configures both, because it cannot
 * tell the two cases apart — enhancers are invoked again for every nested registry, at which point the connector this
 * one registered is itself visible in the enclosing scope. It therefore adds a connector only when none is configured
 * yet, and its {@link #ENHANCER_ORDER order} places it after the enhancers that register the other connectors, so that
 * an explicitly configured connector wins. Choosing between them is a matter of configuration:
 * {@code axon.axonserver.enabled=false} distributes messages through Spring Cloud, and
 * {@code axon.springcloud.enabled=false} distributes them through Axon Server.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class SpringCloudConfigurationEnhancer implements ConfigurationEnhancer {

    private static final Logger logger = LoggerFactory.getLogger(SpringCloudConfigurationEnhancer.class);

    /**
     * The {@link #order()} of this enhancer. Positioned after the Axon Server enhancer
     * ({@code Integer.MIN_VALUE + 10}) and the PostgreSQL enhancer ({@code Integer.MIN_VALUE + 20}), so that a
     * connector either of those registered is already visible when this enhancer decides whether to step aside.
     */
    public static final int ENHANCER_ORDER = Integer.MIN_VALUE + 30;

    @Override
    public void enhance(ComponentRegistry registry) {
        if (!registry.hasComponent(SpringCloudMemberRegistry.class, SearchScope.ALL)) {
            return;
        }
        // Commands and queries are registered independently: a configuration may already carry a connector for one
        // and not the other, and the one it lacks is still worth adding.
        registerCommandConnector(registry);
        registerQueryConnector(registry);
    }

    private static void registerCommandConnector(ComponentRegistry registry) {
        if (registry.hasComponent(CommandBusConnector.class, SearchScope.ALL)) {
            // A connector is already configured here or in an enclosing scope, so none is added. This deliberately
            // does not report a conflict: enhancers are invoked again for each nested registry, and by then the
            // connector registered below is visible in the enclosing scope, so a connector found here is as likely to
            // be this one as another. Which connector distributes commands is settled by configuration instead --
            // see axon.axonserver.enabled and axon.springcloud.enabled.
            logger.debug("A CommandBusConnector is already configured in this or an enclosing scope; "
                                 + "the Spring Cloud connector is not registered again.");
            return;
        }
        registry.registerComponent(commandBusConnectorDefinition())
                .registerDecorator(CommandBusConnector.class, 0, payloadConvertingCommandDecorator());
    }

    private static void registerQueryConnector(ComponentRegistry registry) {
        if (registry.hasComponent(QueryBusConnector.class, SearchScope.ALL)) {
            // Deferred for the same reason a command connector is; see registerCommandConnector.
            logger.debug("A QueryBusConnector is already configured in this or an enclosing scope; "
                                 + "the Spring Cloud connector is not registered again.");
            return;
        }
        registry.registerComponent(queryBusConnectorDefinition())
                .registerDecorator(QueryBusConnector.class, 0, payloadConvertingQueryDecorator());
    }

    private static ComponentDefinition<QueryBusConnector> queryBusConnectorDefinition() {
        return ComponentDefinition.ofType(QueryBusConnector.class)
                                  .withBuilder(config -> new SpringCloudQueryBusConnector(
                                          config.getComponent(SpringCloudMemberRegistry.class),
                                          config.getComponent(IncomingQueryInvoker.class),
                                          config.getComponent(RemoteQueryDispatcher.class),
                                          config.getComponent(MessageConverter.class)
                                  ))
                                  .onStart(Phase.INBOUND_QUERY_CONNECTOR,
                                           connector -> ((SpringCloudQueryBusConnector) connector).start())
                                  .onShutdown(Phase.INBOUND_QUERY_CONNECTOR,
                                              (ComponentLifecycleHandler<QueryBusConnector>) (config, connector) ->
                                                      ((SpringCloudQueryBusConnector) connector).disconnect())
                                  .onShutdown(Phase.OUTBOUND_QUERY_CONNECTORS,
                                              (ComponentLifecycleHandler<QueryBusConnector>) (config, connector) ->
                                                      ((SpringCloudQueryBusConnector) connector)
                                                              .shutdownDispatching());
    }

    private static ComponentDecorator<QueryBusConnector, PayloadConvertingQueryBusConnector>
    payloadConvertingQueryDecorator() {
        return (config, name, delegate) -> new PayloadConvertingQueryBusConnector(
                delegate,
                config.getComponent(MessageConverter.class),
                // Text, not bytes: a query's responses travel as the data of Server-Sent Events, which is text, and
                // the request and reply bodies are read and written as text as well.
                String.class
        );
    }

    private static ComponentDefinition<CommandBusConnector> commandBusConnectorDefinition() {
        return ComponentDefinition.ofType(CommandBusConnector.class)
                                  .withBuilder(config -> new SpringCloudCommandBusConnector(
                                          config.getComponent(SpringCloudMemberRegistry.class),
                                          config.getComponent(IncomingCommandInvoker.class),
                                          config.getComponent(RemoteCommandDispatcher.class),
                                          config.getComponent(MessageConverter.class)
                                  ))
                                  .onStart(Phase.INBOUND_COMMAND_CONNECTOR,
                                           connector -> ((SpringCloudCommandBusConnector) connector).start())
                                  .onShutdown(Phase.INBOUND_COMMAND_CONNECTOR,
                                              (ComponentLifecycleHandler<CommandBusConnector>) (config, connector) ->
                                                      ((SpringCloudCommandBusConnector) connector).disconnect())
                                  .onShutdown(Phase.OUTBOUND_COMMAND_CONNECTORS,
                                              (ComponentLifecycleHandler<CommandBusConnector>) (config, connector) ->
                                                      ((SpringCloudCommandBusConnector) connector)
                                                              .shutdownDispatching());
    }

    private static ComponentDecorator<CommandBusConnector, PayloadConvertingCommandBusConnector>
    payloadConvertingCommandDecorator() {
        return (config, name, delegate) -> new PayloadConvertingCommandBusConnector(
                delegate,
                config.getComponent(MessageConverter.class),
                // Text, not bytes, matching how the query side travels; see the query decorator.
                String.class
        );
    }

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }
}
