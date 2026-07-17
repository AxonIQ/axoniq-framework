package distributedmessaging.distributedquerybus.customize;

// tag::customize-query-bus-configuration[]
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import io.axoniq.framework.messaging.queryhandling.distributed.DistributedQueryBusConfiguration;

public class AxonConfig {
    public void configureQueryBus(MessagingConfigurer configurer) {
        // Customize the configuration
        DistributedQueryBusConfiguration config = DistributedQueryBusConfiguration.DEFAULT
                .queryThreads(20)                        // Set number of query processing threads
                .queryQueueCapacity(2000)                // Set queue capacity
                .preferLocalQueryHandler(true);         // Enable local handler shortcut (default)

        // Register the custom configuration
        configurer.componentRegistry(cr -> cr.registerComponent(DistributedQueryBusConfiguration.class, c -> config));
    }
}
// end::customize-query-bus-configuration[]
