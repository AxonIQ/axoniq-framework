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
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.OrderPlacedEvent;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example workflow demonstrating {@code ctx.migrateVersion(changeId, newVersion)} branching.
 * <p>
 * A new deployment introduces a payment-processing redesign. New workflows bump to version
 * {@code "0.0.2"} via the {@code "payment-redesign"} migration step and take the new branch; in-flight
 * workflows that already executed past the call's position under the old code observe the current
 * version {@code "0.0.1"} (via the downstream-steps guard) and stay on the legacy branch.
 * <p>
 * For larger rewrites, prefer registering a separate workflow class with a bumped
 * {@code @Workflow(workflowVersion="...")} — the runtime routes new instances to the highest version and
 * replays in-flight instances on the version they were started under.
 *
 * @author Stefan Dragisic
 */
public class VersionedWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(VersionedWorkflow.class);

    @Workflow(idProperty = "orderId", startOnEventClass = OrderPlacedEvent.class, workflowVersion = "0.0.1")
    public void execute(@Nonnull SimpleWorkflowContext ctx) {
        ctx.awaitExecute("reserveStock", Boolean.class, () -> {
            logger.info("Reserving stock for {}", ctx.workflowPayload());
            return true;
        });

        if (ctx.migrateVersion("payment-redesign", "0.0.2")) {
            ctx.awaitExecute("processPayment", Boolean.class, () -> {
                logger.info("Processing payment (v0.0.2 branch)");
                return true;
            });
        } else {
            ctx.awaitExecute("chargePayment", Boolean.class, () -> {
                logger.info("Charging payment (legacy v0.0.1 branch)");
                return true;
            });
        }
    }
}
