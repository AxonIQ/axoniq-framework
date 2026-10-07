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
