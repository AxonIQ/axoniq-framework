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

package io.axoniq.framework.messaging.multitenancy.axonserver.configuration;

import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantSnapshotStoreFactory;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Subscribes the effective tenant event-storage and snapshot-store factories to the configured {@link TenantProvider}.
 * <p>
 * Applications can replace either factory. The subscription therefore belongs to this lifecycle component rather than
 * to the default factory definitions: otherwise a replacement that is {@link MultiTenantAwareComponent multi-tenant
 * aware} never receives the tenants already known to the provider.
 *
 * @author Jan Galinski
 * @since 5.4.0
 */
@Internal
class TenantEventStorageComponentSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(TenantEventStorageComponentSubscriber.class);

    private final Configuration configuration;
    private final List<Registration> subscriptions = new CopyOnWriteArrayList<>();

    TenantEventStorageComponentSubscriber(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "The configuration must not be null");
    }

    void subscribeFactories() {
        TenantProvider tenantProvider = configuration.getComponent(TenantProvider.class);
        subscribeIfAware(tenantProvider, configuration.getComponent(TenantSnapshotStoreFactory.class));
        subscribeIfAware(tenantProvider, configuration.getComponent(TenantEventStorageEngineFactory.class));
    }

    private void subscribeIfAware(TenantProvider tenantProvider, Object factory) {
        if (factory instanceof MultiTenantAwareComponent aware) {
            subscriptions.add(tenantProvider.subscribe(aware));
        }
    }

    void cancelSubscriptions() {
        for (Registration subscription : subscriptions) {
            try {
                subscription.cancel();
            } catch (Exception e) {
                logger.warn("Error while cancelling a tenant event-storage component subscription.", e);
            }
        }
        subscriptions.clear();
    }
}
