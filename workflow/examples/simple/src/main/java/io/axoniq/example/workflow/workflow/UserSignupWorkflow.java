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

import io.axoniq.example.workflow.fixture.MagicHappenedEvent;
import io.axoniq.example.workflow.fixture.NotificationService;
import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.example.workflow.fixture.UserService;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.api.annotation.WorkflowCompletedHandler;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static io.axoniq.workflow.dsl.api.AssociationsUtils.associate;
import static io.axoniq.workflow.dsl.api.Payload.payload;
import static io.axoniq.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * Sample user registration.
 */
public class UserSignupWorkflow {

    Logger logger = LoggerFactory.getLogger(UserSignupWorkflow.class);

    @Workflow(
            idProperty = "id",
            startOnEventClass = RegistrationReceivedEvent.class,
            startOnConditions = {"status=vip"}
    )
    public void execute(
            @Nonnull SimpleWorkflowContext ctx
    ) {

        logger.info("User signup workflow started at {} for {}", Instant.now(), ctx.workflowPayload());

        // -> start
        var success = ctx.awaitExecute("createUser", Boolean.class, UserService::createUser);
        if (!success) {
            return;
        }

        ctx.awaitExecute("activateUser",
                         ctx.workflowPayload(),
                         UserService::activateUser,
                         step -> step.timeout(Duration.ofSeconds(10))
        );

        ctx.awaitExecute("sendWelcomeEmail",
                         ctx.workflowPayload(),
                         (pc, input) -> {
                            NotificationService.sendEmail(payload(input).get("email"));
                            return Map.of();
                         });
        ctx.sleep("waitASecond", Duration.ofSeconds(1L));

        var magic = ctx.awaitEvent("waitForMagicToHappen",
                                     MagicHappenedEvent.class,
                                     associate(payloadProperty("magician"), equalsTo("Merlin")),
                                   step -> step.timeout(Duration.ofSeconds(5))
        );
        ctx.setPayload("modifyPayload", magic);

        logger.info("Magic happened because of the magician {}", magic.magician());
        // -> end
    }

    @WorkflowCompletedHandler
    public void onFinish(
            @Nonnull WorkflowStatus workflowStatus,
            @Nonnull SimpleWorkflowContext ctx
    ) {
        logger.info("User signup workflow {} at {} for {}", workflowStatus, Instant.now(), ctx.workflowPayload());
    }
}
