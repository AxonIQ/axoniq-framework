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
package io.axoniq.framework.workflow.dsl.kotlin

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory
import org.axonframework.common.configuration.ComponentRegistry
import org.axonframework.common.configuration.ConfigurationEnhancer

/**
 * ServiceLoader-discovered [ConfigurationEnhancer] that registers the Kotlin DSL workflow context factory.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
class WorkflowKotlinDslConfigurationEnhancer : ConfigurationEnhancer {

    override fun enhance(registry: ComponentRegistry) {
        registry.registerIfNotPresent(
            WorkflowContextFactory::class.java,
            WorkflowKontext::class.java.name,
        ) { WorkflowKontextFactory() }
    }
}
