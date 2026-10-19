/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package distributedmessaging.distributedquerybus.localshortcut;

// tag::configure-local-query-shortcut[]
import io.axoniq.framework.messaging.queryhandling.distributed.LocalQueryDispatchPredicate;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

public class AxonConfig {
    public void configureLocalQueryShortcut(MessagingConfigurer configurer) {
        configurer.componentRegistry(registry -> registry.registerComponent(
                LocalQueryDispatchPredicate.class,
                config -> (query, context) -> true   // always prefer the local handler when subscribed
        ));
    }
}
// end::configure-local-query-shortcut[]
