/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
