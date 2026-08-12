/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.workflow.springboot;

import io.axoniq.workflow.configuration.WorkflowEventProcessingRegistrationEnhancer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Role;

import static io.axoniq.workflow.configuration.WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME;

/**
 * Autoconfiguration for workflow infrastructure.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@AutoConfiguration
public class WorkflowAutoConfiguration {

    /**
     * Provides Spring workflow definition lookup.
     *
     * @return The lookup for annotations for later workflow handling registrations.
     */
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    @Bean
    public static WorkflowDefinitionLookup workflowDefinitionLookup() {
        return new WorkflowDefinitionLookup();
    }

    /**
     * Configures the event handling for the top-level workflow module.
     *
     * @param initialSegmentCount number of segments to initialize the workflow event processor with; workflow
     *                            instances are partitioned over segments by workflow id. Defaults
     *                            to
     *                            {@link WorkflowEventProcessingRegistrationEnhancer#DEFAULT_INITIAL_SEGMENT_COUNT}.
     * @return enhancer.
     */
    @Bean
    @ConditionalOnMissingBean
    public WorkflowEventProcessingRegistrationEnhancer workflowEventHandlersDefaults(
            @Value("${axoniq.workflow.initial-segment-count:4}") int initialSegmentCount
    ) {
        return new WorkflowEventProcessingRegistrationEnhancer(
                DEFAULT_MODULE_NAME, null, null, true, initialSegmentCount
        );
    }
}
