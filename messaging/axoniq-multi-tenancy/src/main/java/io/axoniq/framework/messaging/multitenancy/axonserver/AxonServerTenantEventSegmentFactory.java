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

package io.axoniq.framework.messaging.multitenancy.axonserver;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantEventSegmentFactory;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantRoutingEventStore;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An AxonServer based implementation of the {@link TenantEventSegmentFactory} that creates tenant-specific
 * {@link EventStore}. The {@link TenantRoutingEventStore} will use this to find the correct {@link EventStore} segment
 * for a given {@link TenantDescriptor}.
 * <p>
 * Values are calculated lazily and cached for future use.
 *
 * @author Stefan Dragisic
 * @author Jan Galinski
 * @since 5.3.0
 */
@Internal
public class AxonServerTenantEventSegmentFactory implements TenantEventSegmentFactory {

    /**
     * A {@link ComponentBuilder} for creating an instance of {@code  AxonServerTenantEventSegmentFactory}.
     *
     * @param config the {@link Configuration} to use for resolving components
     * @return the {@code AxonServerTenantEventSegmentFactory} instance
     */
    public static AxonServerTenantEventSegmentFactory buildComponent(Configuration config) {
        return new AxonServerTenantEventSegmentFactory(
                config.getComponent(AxonServerConnectionManager.class),
                config.getComponent(EventConverter.class),
                config.getComponent(SimpleEventBus.class),
                config.getComponent(TagResolver.class, AnnotationBasedTagResolver::new)
        );
    }

    private final Map<TenantDescriptor, EventStore> tenantEventStores = new ConcurrentHashMap<>();
    private final AxonServerConnectionManager connectionManager;
    private final EventConverter eventConverter;
    private final SimpleEventBus localSegment;
    private final TagResolver tagResolver;

    /**
     * Creates a new {@code AxonServerTenantEventSegmentFactory}.,
     *
     * @param connectionManager the {@link AxonServerConnectionManager} to use for creating tenant-specific
     *                          {@link AxonServerConnection}
     * @param eventConverter    the {@link EventConverter} to use for converting events to and from the AxonServer
     *                          format
     * @param localSegment      the {@link SimpleEventBus} to use for local event handling
     * @param tagResolver       the {@link TagResolver} to use for resolving tags for events
     */
    public AxonServerTenantEventSegmentFactory(AxonServerConnectionManager connectionManager,
                                               EventConverter eventConverter,
                                               SimpleEventBus localSegment,
                                               TagResolver tagResolver
    ) {
        this.connectionManager = connectionManager;
        this.eventConverter = eventConverter;
        this.localSegment = localSegment;
        this.tagResolver = tagResolver;
    }

    @Override
    public EventStore apply(TenantDescriptor tenantDescriptor) {
        return tenantEventStores.computeIfAbsent(tenantDescriptor, this::eventStoreForTenant);
    }

    private StorageEngineBackedEventStore eventStoreForTenant(TenantDescriptor tenantDescriptor) {
        String context = tenantDescriptor.tenantId();
        AxonServerConnection connection = connectionManager.getConnection(context);
        AxonServerEventStorageEngine engine = new AxonServerEventStorageEngine(connection, eventConverter);

        return new StorageEngineBackedEventStore(
                engine,
                localSegment,
                tagResolver
        );
    }
}
