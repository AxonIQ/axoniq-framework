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

import io.axoniq.workflow.dsl.kotlin.WorkflowKontextFactory;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration for default workflow DSL context factories.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@AutoConfiguration
public class WorkflowDefaultContextFactoryAutoConfiguration {

    /**
     * Provides a context factory for the Simple workflow DSL.
     *
     * @return context factory.
     */
    @Bean
    public SimpleWorkflowContextFactory simpleWorkflowContextFactory() {
        return new SimpleWorkflowContextFactory();
    }

    /**
     * Provides a context factory for the Kotlin workflow DSL.
     *
     * @return context factory.
     */
    @Bean
    public WorkflowKontextFactory workflowKontextFactory() {
        return new WorkflowKontextFactory();
    }
}
