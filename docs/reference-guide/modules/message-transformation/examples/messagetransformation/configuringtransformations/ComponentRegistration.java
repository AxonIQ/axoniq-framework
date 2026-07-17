package messagetransformation.configuringtransformations;

import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import messagetransformation.coursecatalog.CourseCatalogTransformations;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;

public class ComponentRegistration {

    public void register(EventSourcingConfigurer configurer) {
        // tag::register-declarative[]
        configurer.componentRegistry(registry -> registry
                .registerComponent(EventTransformerChain.class,            // <1>
                                   config -> CourseCatalogTransformations.chain()));
        // end::register-declarative[]
    }
}
