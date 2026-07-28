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

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Subscribes every registered {@link TenantComponentProvider} to the {@link TenantProvider} when the application
 * starts, and cancels those subscriptions when it shuts down.
 * <p>
 * Subscribing at startup makes every provider follow the tenant lifecycle, whether a message handler ever
 * requests it or not. Cancelling the retained subscriptions at shutdown deregisters the tenants registered through them,
 * which destroys the providers' cached component instances without relying on the configured {@link TenantProvider}
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
     * @param configuration the configuration supplying the tenant provider and the tenant-component providers
     */
    TenantComponentProviderSubscriber(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "The configuration must not be null");
    }

    /**
     * Subscribes every registered {@link TenantComponentProvider} to the configured {@link TenantProvider},
     * retaining the subscriptions for {@link #cancelSubscriptions()}.
     */
    // Providers are heterogeneous in their component type, so they are looked up through their raw type.
    @SuppressWarnings("rawtypes")
    void subscribeProviders() {
        TenantProvider tenantProvider = configuration.getComponent(TenantProvider.class);
        for (TenantComponentProvider provider : configuration.getComponents(TenantComponentProvider.class).values()) {
            subscriptions.add(tenantProvider.subscribe(provider));
        }
    }

    /**
     * Cancels the retained subscriptions, deregistering every tenant that was registered through them and thereby
     * destroying the providers' cached component instances.
     * <p>
     * A failing cancellation is logged and skipped, so the remaining subscriptions are still cancelled.
     */
    void cancelSubscriptions() {
        for (Registration subscription : subscriptions) {
            try {
                subscription.cancel();
            } catch (Exception e) {
                logger.warn("Error while cancelling a tenant component provider subscription.", e);
            }
        }
        subscriptions.clear();
    }
}
