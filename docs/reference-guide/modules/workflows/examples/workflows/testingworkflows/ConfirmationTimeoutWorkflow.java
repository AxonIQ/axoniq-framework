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
