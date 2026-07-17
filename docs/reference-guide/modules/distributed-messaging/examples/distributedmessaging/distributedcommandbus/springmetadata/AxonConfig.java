package distributedmessaging.distributedcommandbus.springmetadata;

import org.axonframework.messaging.commandhandling.RoutingStrategy;
import org.axonframework.messaging.commandhandling.MetadataRoutingStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// tag::metadata-routing-strategy-bean[]
@Configuration
public class AxonConfig {

    @Bean
    public RoutingStrategy routingStrategy() {
        return new MetadataRoutingStrategy("routingKey");
    }
}
// end::metadata-routing-strategy-bean[]
