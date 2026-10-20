/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package distributedmessaging.distributedcommandbus.localshortcut;

// tag::configure-local-command-shortcut[]
import io.axoniq.framework.messaging.commandhandling.distributed.LocalCommandDispatchPredicate;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

public class AxonConfig {
    public void configureLocalCommandShortcut(MessagingConfigurer configurer) {
        configurer.componentRegistry(registry -> registry.registerComponent(
                LocalCommandDispatchPredicate.class,
                config -> (command, context) -> true   // prefer the local handler when subscribed
        ));
    }
}
// end::configure-local-command-shortcut[]
