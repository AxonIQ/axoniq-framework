package distributedmessaging.distributedquerybus.defaultconfig;

// tag::default-configuration[]
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

public class AxonApp {

    public static void main(String[] args) {
        // Returns a Configurer instance with default components configured.
        // `DistributedQueryBus` with `AxonServerQueryBusConnector` is configured as Query Bus by default.
        MessagingConfigurer configurer = MessagingConfigurer.create();
    }
}
// end::default-configuration[]
