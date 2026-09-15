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

package workflows.publishingevents;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;

public class ShipmentWorkflow {

    // tag::workflow-to-workflow[]
    // Shipment workflow, started on OrderApproved
    @Workflow(idProperty = "orderId",
              startOnEventClass = OrderApproved.class,
              workflowNamespace = "io.myapp.shipping")
    public void execute(SimpleWorkflowContext ctx) {
        ctx.awaitExecute("prepareShipment", Boolean.class, () -> true);
    }
    // end::workflow-to-workflow[]
}
