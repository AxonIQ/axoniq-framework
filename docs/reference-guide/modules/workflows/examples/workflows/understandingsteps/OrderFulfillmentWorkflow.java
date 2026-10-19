/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.understandingsteps;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

import java.util.Map;

public class OrderFulfillmentWorkflow {

    // tag::workflow-level-namespace[]
    @Workflow(
            idProperty = "orderId",
            startOnEventClass = OrderPlacedEvent.class,
            workflowNamespace = "io.myapp.orders"   // <1>
    )
    public void execute(SimpleWorkflowContext context) {
        context.awaitExecute(
                "reserveStock",
                Map.of(),
                (pc, input) -> Map.of("reserved", InventoryService.reserveStock()) // <2>
        );
    }
    // end::workflow-level-namespace[]
}
