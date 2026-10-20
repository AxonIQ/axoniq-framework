/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.versioning;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

public class MigrateVersionWorkflow {

    // tag::migrate-version-contract[]
    @Workflow(
            idProperty = "orderId",
            startOnEventClass = OrderPlacedEvent.class,
            workflowVersion = "0.0.1"
    )
    public void execute(SimpleWorkflowContext context) {
        context.awaitExecute(
                "reserveStock",
                Boolean.class,
                InventoryService::reserveStock
        );

        if (context.migrateVersion("payment-redesign", "0.0.2")) {
            context.awaitExecute(
                    "processPayment",
                    Boolean.class,
                    PaymentService::processV2
            );
        } else {
            context.awaitExecute(
                    "chargePayment",
                    Boolean.class,
                    PaymentService::chargeV1
            );
        }
    }
    // end::migrate-version-contract[]

    public void threePhaseCleanup(SimpleWorkflowContext context) {
        // tag::three-phase-cleanup[]
        // Phase 1: introduce the change. Both branches live.
        if (context.migrateVersion("payment-redesign", "0.0.2")) {
            newer();
        } else {
            old();
        }

        // Phase 2: once all v1 workflows have drained, delete the old branch.
        //          The migrate call stays. New workflows still record the step.
        context.migrateVersion("payment-redesign", "0.0.2");
        newer();

        // Phase 3: once no further versioning is anticipated, delete the call.
        //          Orphan migration steps in old event logs are inert, Axon's replay tolerates them.
        newer();
        // end::three-phase-cleanup[]
    }

    public void multiVersioned(SimpleWorkflowContext context) {
        // tag::multi-versioned[]
        boolean payV2 = context.migrateVersion("payment-redesign", "0.0.2");
        boolean shipV2 = context.migrateVersion("shipping-redesign", "0.0.3");
        // Each changeId is its own slot in state.versions, neither call affects the other.
        // end::multi-versioned[]
    }

    private void newer() {
        // ...
    }

    private void old() {
        // ...
    }
}
