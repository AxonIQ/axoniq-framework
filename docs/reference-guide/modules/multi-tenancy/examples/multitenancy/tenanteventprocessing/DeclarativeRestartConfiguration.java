/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenanteventprocessing;

import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenantStreamingProcessorRestartConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

import java.time.Duration;

public class DeclarativeRestartConfiguration {

    // tag::restart-timeout[]
    public void raiseRestartTimeout(MessagingConfigurer configurer) {
        configurer.componentRegistry(registry ->
            registry.registerComponent(
                    MultiTenantStreamingProcessorRestartConfiguration.class,
                    config -> MultiTenantStreamingProcessorRestartConfiguration.DEFAULT
                            .restartTimeout(Duration.ofMinutes(2))                          // <1>
            )
        );
    }
    // end::restart-timeout[]
}
