/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.configuration;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

public class AnnotationExampleWorkflow {

    // tag::annotation-example[]
    @Workflow(
            idProperty = "orderId",
            startOnEventClass = OrderPlacedEvent.class,
            workflowNamespace = "io.myapp.orders"
    )
    public void execute(SimpleWorkflowContext workflowContext) { /* ... */ }
    // end::annotation-example[]
}
