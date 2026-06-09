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

package io.axoniq.framework.messaging.multitenancy.command;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.command.AxonServerCommandBusConnector;
import io.axoniq.framework.messaging.commandhandling.distributed.DistributedCommandBus;
import io.axoniq.framework.messaging.commandhandling.distributed.DistributedCommandBusConfiguration;
import io.axoniq.framework.messaging.commandhandling.distributed.PayloadConvertingCommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.command.TenantCommandSegmentFactory;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.SimpleCommandBus;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;

import java.util.Objects;

/**
 * Creates a tenant specific {@link CommandBus} backed by Axon Server.
 * <p>
 * Each tenant is mapped to a dedicated Axon Server context by its tenant identifier. The implementation reuses the
 * shared {@link AxonServerConfiguration} and resolves the tenant specific {@link io.axoniq.axonserver.connector.AxonServerConnection}
 * through the shared {@link AxonServerConnectionManager}.
 *
 * @author Jan Galinski
 * @since 5.0.0
 */
public class AxonServerTenantCommandSegmentFactory implements TenantCommandSegmentFactory {

    private final AxonServerConnectionManager connectionManager;
    private final AxonServerConfiguration axonServerConfiguration;
    private final MessageConverter messageConverter;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final DistributedCommandBusConfiguration distributedCommandBusConfiguration;

    /**
     * Creates a tenant command segment factory using the default {@link UnitOfWorkFactory} and distributed command
     * bus configuration.
     *
     * @param connectionManager       The shared Axon Server connection manager.
     * @param axonServerConfiguration  The shared Axon Server configuration.
     * @param messageConverter         The message converter used to serialize command payloads.
     */
    public AxonServerTenantCommandSegmentFactory(AxonServerConnectionManager connectionManager,
                                                 AxonServerConfiguration axonServerConfiguration,
                                                 MessageConverter messageConverter) {
        this(connectionManager,
             axonServerConfiguration,
             messageConverter,
             new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE),
             DistributedCommandBusConfiguration.DEFAULT);
    }

    /**
     * Creates a tenant command segment factory.
     *
     * @param connectionManager                   The shared Axon Server connection manager.
     * @param axonServerConfiguration              The shared Axon Server configuration.
     * @param messageConverter                     The message converter used to serialize command payloads.
     * @param unitOfWorkFactory                    The {@link UnitOfWorkFactory} used by each tenant's local command bus.
     * @param distributedCommandBusConfiguration   The distributed command bus configuration.
     */
    public AxonServerTenantCommandSegmentFactory(AxonServerConnectionManager connectionManager,
                                                 AxonServerConfiguration axonServerConfiguration,
                                                 MessageConverter messageConverter,
                                                 UnitOfWorkFactory unitOfWorkFactory,
                                                 DistributedCommandBusConfiguration distributedCommandBusConfiguration) {
        this.connectionManager = Objects.requireNonNull(connectionManager, "The connectionManager must not be null.");
        this.axonServerConfiguration = Objects.requireNonNull(
                axonServerConfiguration,
                "The axonServerConfiguration must not be null."
        );
        this.messageConverter = Objects.requireNonNull(messageConverter, "The messageConverter must not be null.");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "The unitOfWorkFactory must not be null.");
        this.distributedCommandBusConfiguration = Objects.requireNonNull(
                distributedCommandBusConfiguration,
                "The distributedCommandBusConfiguration must not be null."
        );
    }

    @Override
    public CommandBus apply(TenantDescriptor tenantDescriptor) {
        String context = tenantDescriptor.tenantId();
        var connection = connectionManager.getConnection(context);
        var connector = new PayloadConvertingCommandBusConnector(
                new AxonServerCommandBusConnector(connection, axonServerConfiguration),
                messageConverter,
                byte[].class
        );
        var localCommandBus = new SimpleCommandBus(unitOfWorkFactory);
        return new DistributedCommandBus(localCommandBus, connector, distributedCommandBusConfiguration);
    }
}
