/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package distributedmessaging.distributedcommandbus.routingstrategy;

import org.axonframework.messaging.commandhandling.RoutingStrategy;
import org.axonframework.messaging.commandhandling.annotation.AnnotationRoutingStrategy;

// tag::routing-strategy-config[]
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

public class AxonConfig {

    public void configureRoutingStrategy() {
        MessagingConfigurer configurer = MessagingConfigurer.create();

        configurer.componentRegistry(registry ->
            registry.registerComponent(
                RoutingStrategy.class,
                config -> new AnnotationRoutingStrategy()
            )
        );
    }
}
// end::routing-strategy-config[]
