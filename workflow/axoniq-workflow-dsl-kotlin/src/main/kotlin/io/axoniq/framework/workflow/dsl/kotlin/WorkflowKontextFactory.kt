/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.kotlin

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory
import org.axonframework.messaging.core.unitofwork.ProcessingContext

/**
 * Factory that creates [WorkflowKontext] instances for Kotlin DSL workflows.
 *
 * @since 5.4.0
 * @author Simon Zambrovski
 */
class WorkflowKontextFactory : WorkflowContextFactory<WorkflowKontext> {

    /**
     * Creates a workflow context for the current workflow invocation.
     *
     * @param initialPayload initial workflow payload
     * @param workflowId unique identifier of the workflow instance
     * @param processingContext processing context for the current message
     * @param workflowConfiguration runtime configuration for this workflow
     * @return workflow context passed to the Kotlin DSL definition
     */
    override fun createContext(
        initialPayload: Map<String, Any?>,
        workflowId: String,
        processingContext: ProcessingContext,
        workflowConfiguration: WorkflowConfiguration<*>,
    ): WorkflowKontext = WorkflowKontext(
        initialPayload = initialPayload,
        workflowId = workflowId,
        processingContext = processingContext,
        workflowConfiguration = workflowConfiguration,
    )
}
