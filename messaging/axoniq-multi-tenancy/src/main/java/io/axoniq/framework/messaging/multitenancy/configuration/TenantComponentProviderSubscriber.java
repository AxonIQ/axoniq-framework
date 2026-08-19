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
 * https://www.axoniq.io/pricing
 */

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
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
 * Subscribes registered {@link MultiTenantAwareComponent multi-tenant-aware components} to the
 * {@link TenantProvider} when the application starts, and cancels those subscriptions when it shuts down.
 * <p>
 * Subscribing at startup makes every provider and tenant-aware storage factory follow the tenant lifecycle, whether a
 * message handler ever requests it or not. Cancelling the retained subscriptions at shutdown deregisters the tenants
 * registered through them,
 * which destroys the components' cached tenant instances without relying on the configured {@link TenantProvider}
 * to deregister its subscribers on its own.
 * <p>
 * Internal, because it is registered by the {@link MultiTenancyConfigurationDefaults} enhancer and never used
 * directly.
 *
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
class TenantComponentProviderSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(TenantComponentProviderSubscriber.class);

    private final Configuration configuration;
    private final List<Registration> subscriptions = new CopyOnWriteArrayList<>();

    /**
     * Constructs a subscriber for the given {@code configuration}.
     *
     * @param configuration the configuration supplying the tenant provider and tenant-aware components
     */
    TenantComponentProviderSubscriber(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "The configuration must not be null");
    }

    /**
     * Subscribes every registered {@link TenantComponentProvider} and tenant-aware storage factory to the configured
     * {@link TenantProvider}, retaining the subscriptions for {@link #cancelSubscriptions()}.
     */
    @SuppressWarnings("rawtypes") // Providers are heterogeneous in their component type.
    void subscribeComponents() {
        subscribeComponents(configuration.getComponents(TenantComponentProvider.class).values().toArray());
        subscribeComponentIfPresent(TenantSnapshotStoreFactory.class);
        subscribeComponentIfPresent(TenantEventStorageEngineFactory.class);
    }

    /**
     * Subscribes the effective component registered under the given type when it is
     * {@link MultiTenantAwareComponent multi-tenant aware}, retaining the subscription for
     * {@link #cancelSubscriptions()}.
     *
     * @param componentType the type under which the component is registered
     */
    private void subscribeComponentIfPresent(Class<?> componentType) {
        if (configuration.hasComponent(componentType)) {
            subscribeComponents(configuration.getComponent(componentType));
        }
    }

    private void subscribeComponents(Object... components) {
        TenantProvider tenantProvider = configuration.getComponent(TenantProvider.class);
        for (Object component : components) {
            if (component instanceof MultiTenantAwareComponent aware) {
                subscriptions.add(tenantProvider.subscribe(aware));
            }
        }
    }

    /**
     * Cancels the retained subscriptions, deregistering every tenant that was registered through them and thereby
     * destroying the components' cached tenant instances.
     * <p>
     * A failing cancellation is logged and skipped, so the remaining subscriptions are still cancelled.
     */
    void cancelSubscriptions() {
        for (Registration subscription : subscriptions) {
            try {
                subscription.cancel();
            } catch (Exception e) {
                logger.warn("Error while cancelling a tenant-aware component subscription.", e);
            }
        }
        subscriptions.clear();
    }
}
