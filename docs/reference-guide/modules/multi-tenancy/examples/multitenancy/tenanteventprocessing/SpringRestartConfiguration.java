/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenanteventprocessing;

import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenantStreamingProcessorRestartConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class SpringRestartConfiguration {

    // tag::restart-timeout[]
    @Bean
    public MultiTenantStreamingProcessorRestartConfiguration processorRestartConfiguration() {
        return MultiTenantStreamingProcessorRestartConfiguration.DEFAULT
                .restartTimeout(Duration.ofMinutes(2));                                 // <1>
    }
    // end::restart-timeout[]
}
