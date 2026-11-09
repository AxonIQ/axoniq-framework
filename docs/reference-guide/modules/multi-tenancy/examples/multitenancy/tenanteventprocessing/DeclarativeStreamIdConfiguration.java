/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenanteventprocessing;

import io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming.MultiTenantPersistentStreamIdResolver;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

public class DeclarativeStreamIdConfiguration {

    // tag::stream-id-resolver[]
    public void resolveStreamIdPerTenant(MessagingConfigurer configurer) {
        configurer.componentRegistry(registry ->
            registry.registerComponent(
                    MultiTenantPersistentStreamIdResolver.class,
                    config -> (name, tenant) -> name + "-" + tenant.tenantId()      // <1>
            )
        );
    }
    // end::stream-id-resolver[]
}
