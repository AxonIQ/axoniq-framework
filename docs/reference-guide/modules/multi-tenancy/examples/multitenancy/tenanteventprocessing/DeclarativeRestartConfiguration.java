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
