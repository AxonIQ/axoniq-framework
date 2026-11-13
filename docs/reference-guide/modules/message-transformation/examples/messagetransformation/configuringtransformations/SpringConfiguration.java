/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

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
