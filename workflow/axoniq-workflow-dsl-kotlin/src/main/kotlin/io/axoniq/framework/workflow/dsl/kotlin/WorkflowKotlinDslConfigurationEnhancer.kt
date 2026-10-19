/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
