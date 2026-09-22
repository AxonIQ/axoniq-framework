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
