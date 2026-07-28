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

package distributedmessaging.distributedcommandbus.springannotation;

import org.axonframework.messaging.commandhandling.RoutingStrategy;
import org.axonframework.messaging.commandhandling.annotation.AnnotationRoutingStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// tag::annotation-routing-strategy-bean[]
@Configuration
public class AxonConfig {

    @Bean
    public RoutingStrategy routingStrategy() {
        // Use default (Command.class annotation)
        return new AnnotationRoutingStrategy();
    }
}
// end::annotation-routing-strategy-bean[]
