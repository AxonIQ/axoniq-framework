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
