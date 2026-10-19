/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.publishingevents;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.annotation.Workflow;

public class ShipmentWorkflow {

    // tag::workflow-to-workflow[]
    // Shipment workflow, started on OrderApproved
    @Workflow(idProperty = "orderId",
              startOnEventClass = OrderApproved.class,
              workflowNamespace = "io.myapp.shipping")
    public void execute(SimpleWorkflowContext workflowContext) {
        workflowContext.awaitExecute("prepareShipment", Boolean.class, () -> true);
    }
    // end::workflow-to-workflow[]
}
