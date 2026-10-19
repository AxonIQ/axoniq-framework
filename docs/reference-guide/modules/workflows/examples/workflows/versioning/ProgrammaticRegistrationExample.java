/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.versioning;

import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;

public class ProgrammaticRegistrationExample {

    public void register() {
        var orderWorkflow = new OrderWorkflow();

        // tag::programmatic-registration[]
        WorkflowModule.defaults("OrderWorkflow", SimpleWorkflowContext.class).definition(
                d -> d.declarative(c -> orderWorkflow::execute)
                      .workflowName("OrderWorkflow")
                      .on(EventConditions.fromType(OrderPlacedEvent.class))
                      .customized((c, w) -> w.workflowVersion("0.0.2")
                                             .workflowIdProvider(new PayloadPropertyWorkflowIdProvider("orderId")))
        );
        // end::programmatic-registration[]
    }
}
