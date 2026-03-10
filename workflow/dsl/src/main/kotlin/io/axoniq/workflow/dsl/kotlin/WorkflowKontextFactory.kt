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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.dsl.kotlin

import io.axoniq.workflow.runtime.api.EventNameCustomizer
import io.axoniq.workflow.runtime.api.WorkflowContextFactory
import org.axonframework.messaging.core.unitofwork.ProcessingContext

/**
 * Kotlin Kontext factory.
 * @since 1.0.0
 * @author Simon Zambrovski
 */
class WorkflowKontextFactory : WorkflowContextFactory<WorkflowKontext> {

    override fun createContext(
        initialPayload: Map<String, Any?>,
        workflowId: String,
        processingContext: ProcessingContext,
        eventNameCustomizer: EventNameCustomizer
    ): WorkflowKontext = WorkflowKontext(
        initialPayload = initialPayload,
        workflowId = workflowId,
        processingContext = processingContext,
        parentCustomizer = eventNameCustomizer
    )
}