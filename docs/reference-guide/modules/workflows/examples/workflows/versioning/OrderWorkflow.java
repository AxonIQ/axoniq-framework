/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.versioning;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

public class OrderWorkflow {

    // tag::definition-level-version[]
    @Workflow(
            idProperty = "orderId",
            startOnEventClass = OrderPlacedEvent.class,
            workflowName = "OrderWorkflow",
            workflowVersion = "0.0.2"
    )
    public void execute(SimpleWorkflowContext context) { /* ... */ }
    // end::definition-level-version[]
}
