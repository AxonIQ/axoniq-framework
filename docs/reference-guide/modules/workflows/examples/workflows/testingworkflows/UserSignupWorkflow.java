/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.testingworkflows;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

import java.time.Duration;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.payloadProperty;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;

public class UserSignupWorkflow {

    @Workflow(
            idProperty = "id",
            startOnEventClass = RegistrationReceivedEvent.class,
            workflowName = "UserSignup"
    )
    public void execute(SimpleWorkflowContext context) {
        context.awaitExecute("createUser", context.workflowPayload(), UserService::createUser);
        context.awaitExecute("activateUser", context.workflowPayload(), UserService::activateUser);
        context.awaitExecute("sendWelcomeEmail", context.workflowPayload(), EmailService::sendWelcomeEmail);

        var magicEvent = context.awaitEvent(
                "waitForMagicToHappen",
                MagicHappenedEvent.class,
                associate(payloadProperty("magician"), equalsTo(context.workflowId())),
                step -> step.timeout(Duration.ofDays(30))
        );

        context.setPayload("modifyPayload", magicEvent);
    }
}
