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

package distributedmessaging.distributedcommandbus.configure;

// tag::configure-distributed-command-bus[]
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.commandhandling.SimpleCommandBus;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.commandhandling.distributed.DistributedCommandBus;
import io.axoniq.framework.messaging.commandhandling.distributed.DistributedCommandBusConfiguration;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;

public class AxonConfig {

    public void configureDistributedCommandBus(MessagingConfigurer configurer) {
        configurer.registerCommandBus(
                config -> {
                    SimpleCommandBus localSegment = new SimpleCommandBus(
                            config.getComponent(UnitOfWorkFactory.class)
                    );
                    return new DistributedCommandBus(
                            localSegment,
                            config.getComponent(CommandBusConnector.class),
                            DistributedCommandBusConfiguration.DEFAULT
                    );
                }
        );
    }
}
// end::configure-distributed-command-bus[]
