/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.configuration.spring;

// tag::workflow-index-bean[]
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class WorkflowIndexConfig {

    @Bean
    public JpaWorkflowIndex workflowIndex(EntityManagerFactory entityManagerFactory) { // <1>
        return JpaWorkflowIndex.builder() // <2>
                               .entityManagerFactory(entityManagerFactory)
                               .timeout(Duration.ofSeconds(5))
                               .build();
    }
}
// end::workflow-index-bean[]
