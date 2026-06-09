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

package io.axoniq.framework.messaging.multitenancy.query;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.query.AxonServerQueryBusConnector;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.query.TenantQuerySegmentFactory;
import io.axoniq.framework.messaging.queryhandling.distributed.DistributedQueryBus;
import io.axoniq.framework.messaging.queryhandling.distributed.DistributedQueryBusConfiguration;
import io.axoniq.framework.messaging.queryhandling.distributed.PayloadConvertingQueryBusConnector;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.SimpleQueryBus;

import java.util.Objects;

/**
 * Creates a tenant specific {@link QueryBus} backed by Axon Server.
 * <p>
 * Each tenant is mapped to a dedicated Axon Server context by its tenant identifier. The implementation reuses the
 * shared {@link AxonServerConfiguration} and resolves the tenant specific
 * {@link io.axoniq.axonserver.connector.AxonServerConnection} through the shared
 * {@link AxonServerConnectionManager}.
 *
 * @author Jan Galinski
 * @since 5.0.0
 */
public class AxonServerTenantQuerySegmentFactory implements TenantQuerySegmentFactory {

    private final AxonServerConnectionManager connectionManager;
    private final AxonServerConfiguration axonServerConfiguration;
    private final MessageConverter messageConverter;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final DistributedQueryBusConfiguration distributedQueryBusConfiguration;

    /**
     * Creates a tenant query segment factory using the default {@link UnitOfWorkFactory} and distributed query bus
     * configuration.
     *
     * @param connectionManager      the shared Axon Server connection manager
     * @param axonServerConfiguration the shared Axon Server configuration
     * @param messageConverter        the message converter used to serialize query payloads
     */
    public AxonServerTenantQuerySegmentFactory(AxonServerConnectionManager connectionManager,
                                               AxonServerConfiguration axonServerConfiguration,
                                               MessageConverter messageConverter) {
        this(connectionManager,
             axonServerConfiguration,
             messageConverter,
             new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE),
             DistributedQueryBusConfiguration.DEFAULT);
    }

    /**
     * Creates a tenant query segment factory.
     *
     * @param connectionManager                  the shared Axon Server connection manager
     * @param axonServerConfiguration             the shared Axon Server configuration
     * @param messageConverter                    the message converter used to serialize query payloads
     * @param unitOfWorkFactory                   the {@link UnitOfWorkFactory} used by each tenant's local query bus
     * @param distributedQueryBusConfiguration    the distributed query bus configuration
     */
    public AxonServerTenantQuerySegmentFactory(AxonServerConnectionManager connectionManager,
                                               AxonServerConfiguration axonServerConfiguration,
                                               MessageConverter messageConverter,
                                               UnitOfWorkFactory unitOfWorkFactory,
                                               DistributedQueryBusConfiguration distributedQueryBusConfiguration) {
        this.connectionManager = Objects.requireNonNull(connectionManager, "The connectionManager must not be null.");
        this.axonServerConfiguration = Objects.requireNonNull(
                axonServerConfiguration,
                "The axonServerConfiguration must not be null."
        );
        this.messageConverter = Objects.requireNonNull(messageConverter, "The messageConverter must not be null.");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "The unitOfWorkFactory must not be null.");
        this.distributedQueryBusConfiguration = Objects.requireNonNull(
                distributedQueryBusConfiguration,
                "The distributedQueryBusConfiguration must not be null."
        );
    }

    @Override
    public QueryBus apply(TenantDescriptor tenantDescriptor) {
        String context = tenantDescriptor.tenantId();
        var connection = connectionManager.getConnection(context);
        var axonServerQueryBusConnector = new AxonServerQueryBusConnector(
                connection,
                axonServerConfiguration,
                messageConverter
        );
        axonServerQueryBusConnector.start();
        var connector = new PayloadConvertingQueryBusConnector(
                axonServerQueryBusConnector,
                messageConverter,
                byte[].class
        );
        var localQueryBus = new SimpleQueryBus(unitOfWorkFactory);
        return new DistributedQueryBus(localQueryBus, connector, distributedQueryBusConfiguration);
    }
}
