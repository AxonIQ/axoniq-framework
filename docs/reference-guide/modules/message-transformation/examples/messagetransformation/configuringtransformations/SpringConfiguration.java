package messagetransformation.configuringtransformations;

import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import messagetransformation.coursecatalog.CourseCatalogTransformations;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringConfiguration {

    // tag::register-spring[]
    @Bean
    public EventTransformerChain eventTransformerChain() {
        return CourseCatalogTransformations.chain();
    }
    // end::register-spring[]
}
