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

package distributedmessaging.distributedquerybus.customize;

// tag::customize-query-bus-configuration[]
import io.axoniq.framework.messaging.queryhandling.distributed.DistributedQueryBusConfiguration;
import io.axoniq.framework.messaging.queryhandling.distributed.LocalQueryDispatchPredicate;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

public class AxonConfig {
    public void configureQueryBus(MessagingConfigurer configurer) {
        // Customize the configuration
        DistributedQueryBusConfiguration config = DistributedQueryBusConfiguration.DEFAULT
                .queryThreads(20)                        // Set number of query processing threads
                .queryQueueCapacity(2000)                // Set queue capacity
                .preferLocalQueryHandler(true);         // Enable local handler shortcut (default)

        // Register the custom configuration
        configurer.componentRegistry(
                cr -> cr.registerComponent(DistributedQueryBusConfiguration.class, c -> config)
                        .registerComponent(
                                LocalQueryDispatchPredicate.class,
                                c -> (query, context) -> true
                        )
        );
    }
}
// end::customize-query-bus-configuration[]
