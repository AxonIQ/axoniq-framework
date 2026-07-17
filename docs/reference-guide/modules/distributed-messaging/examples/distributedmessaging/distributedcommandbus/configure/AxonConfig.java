package distributedmessaging.distributedcommandbus.configure;

// tag::configure-distributed-command-bus[]
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.commandhandling.CommandBus;
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
