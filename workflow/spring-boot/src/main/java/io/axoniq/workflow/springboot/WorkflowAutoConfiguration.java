/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.springboot;

import io.axoniq.workflow.configuration.WorkflowConfigurerDefaults;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Role;

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

    @Bean
    public ConfigurationEnhancer workflowConfigurationDefaults() {
        return new WorkflowConfigurerDefaults();
    }
}
