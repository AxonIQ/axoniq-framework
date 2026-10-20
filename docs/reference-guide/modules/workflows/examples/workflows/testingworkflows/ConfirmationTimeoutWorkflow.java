/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.testingworkflows;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

import java.time.Duration;
import java.util.Map;

public class ConfirmationTimeoutWorkflow {

    @Workflow(
            idProperty = "id",
            startOnEventClass = PaymentRequested.class,
            workflowName = "ConfirmationTimeout"
    )
    public void execute(SimpleWorkflowContext context) {
        // tag::timeout-workflow-body[]
        var confirmation = context.waitForEvent(
                "waitForConfirmation",
                EventConditions.never(),
                step -> step.timeout(Duration.ofMinutes(5))
        );
        confirmation.await();
        if (confirmation.timeout()) {
            context.awaitExecute(
                    "handleTimeout",
                    Map.of(),
                    (pc, payload) -> Map.of("handledBy", "workflow")
            );
        }
        // end::timeout-workflow-body[]
    }
}
