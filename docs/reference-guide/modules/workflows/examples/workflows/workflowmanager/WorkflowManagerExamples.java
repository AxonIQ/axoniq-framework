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

package workflows.workflowmanager;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowInstance;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.VersionedType;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class WorkflowManagerExamples {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowManagerExamples.class);

    public WorkflowManager retrieveManager(Configuration configuration, String workflowModuleName) {
        // tag::retrieve-manager[]
        WorkflowManager workflowManager =
                configuration.getComponents(WorkflowManager.class)
                             .get("WorkflowManager[" + workflowModuleName + "]");
        // end::retrieve-manager[]
        return workflowManager;
    }

    public void findOne(WorkflowManager workflowManager, String orderId) {
        // tag::find-one[]
        workflowManager.findOne(WorkflowStateQuery.byWorkflowId(orderId))
                       .singleState()
                       .thenAccept(state -> {
                           if (state != null) {
                               use(state);
                           }
                       });
        // end::find-one[]
    }

    public void findMany(WorkflowManager workflowManager) {
        // tag::find-many[]
        var paymentWorkflows = workflowManager.findMany(
                WorkflowStateQuery.byWorkflowDefinitionId(VersionedType.of("PaymentWorkflow", "1.0"))
                                  .workflowStatus(WorkflowStatus.STARTED)
        );

        CompletableFuture<Integer> count = paymentWorkflows.size();
        Publisher<WorkflowInstance> instances = paymentWorkflows.instances();
        // end::find-many[]
    }

    public void requestCancellation(WorkflowManager workflowManager, String orderId) {
        // tag::request-cancellation[]
        var order = workflowManager.findOne(WorkflowStateQuery.byWorkflowId(orderId));

        // Cancel one running step. The future is true only when a terminal step cancellation was recorded.
        CompletableFuture<Boolean> stepCancelled = order.requestStepCancellation("awaitApproval", null);

        // Cancel all currently running steps but leave the workflow active.
        CompletableFuture<Integer> cancelledSteps = order.requestCancellationOfAllSteps(null);

        // Cancel the workflow. The future completes after its cancellation event is durable and its body is woken.
        CompletableFuture<Void> workflowCancelled = order.requestWorkflowCancellation(null);
        // end::request-cancellation[]
    }

    private void use(WorkflowState state) {
        LOGGER.info("Customer id is: {}", state.payload().get("customerId"));
    }
}
