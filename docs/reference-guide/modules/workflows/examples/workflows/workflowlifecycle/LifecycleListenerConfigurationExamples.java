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

package workflows.workflowlifecycle;

import io.axoniq.framework.workflow.configuration.WorkflowCustomization;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LifecycleListenerConfigurationExamples {

    private static final Logger logger = LoggerFactory.getLogger(LifecycleListenerConfigurationExamples.class);

    public void programmaticListeners() {
        var workflow = new OrderFulfillmentWorkflow();

        WorkflowModule.defaults("order-workflows", SimpleWorkflowContext.class).definition(
                d -> d.declarative(c -> workflow::execute)
                      .workflowName("OrderFulfillment")
                      .on(EventConditions.fromType(OrderPlacedEvent.class))
                      // tag::programmatic-listeners[]
                      .customized((c, w) -> w.registerWorkflowStatusChangeListener(
                                                     WorkflowStatus.COMPLETED,
                                                     (status, context, processingContext) -> {
                                                         logger.info("Workflow {} completed",
                                                                     context.workflowId());
                                                     }
                                             )
                                             .registerWorkflowStatusChangeListener(
                                                     WorkflowStatus.FAILED,
                                                     (status, context, processingContext) -> {
                                                         logger.warn("Workflow {} failed",
                                                                     context.workflowId());
                                                     }
                                             ))
                // end::programmatic-listeners[]
        );
    }

    public void unregisterListener(WorkflowCustomization w, WorkflowStatusChangeListener myListener) {
        // tag::unregister-listener[]
        w.unregisterWorkflowStatusChangeListener(WorkflowStatus.COMPLETED, myListener);
        // end::unregister-listener[]
    }
}
