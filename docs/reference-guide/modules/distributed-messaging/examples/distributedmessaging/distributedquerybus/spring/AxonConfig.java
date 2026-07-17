package distributedmessaging.distributedquerybus.spring;

// tag::query-bus-configuration-bean[]
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import io.axoniq.framework.messaging.queryhandling.distributed.DistributedQueryBusConfiguration;

@Configuration
public class AxonConfig {

    @Bean
    public DistributedQueryBusConfiguration queryBusConfiguration() {
        return DistributedQueryBusConfiguration.DEFAULT
                .queryThreads(20)
                .preferLocalQueryHandler(true);
    }
}
// end::query-bus-configuration-bean[]
