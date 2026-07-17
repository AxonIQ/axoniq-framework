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
