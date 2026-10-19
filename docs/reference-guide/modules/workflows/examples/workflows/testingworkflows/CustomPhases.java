/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.testingworkflows;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.test.fixture.GivenWhen;
import io.axoniq.framework.workflow.runtime.test.fixture.Then;

import java.util.Map;

// tag::custom-phases[]
class SignupGivenWhen extends GivenWhen<SignupGivenWhen, SignupThen> {

    SignupGivenWhen vipRegistrationReceived(String id, String email) {
        return publishEvent(new RegistrationReceivedEvent(id, email, "vip"));
    }

    SignupGivenWhen customerRegistrationCompleted() {
        return executionExists()
                .execute("createUser")
                .executeReturning("activateUser", Map.of())
                .executeReturning("sendWelcomeEmail", Map.of());
    }

    SignupGivenWhen magicHappens(String magician) {
        return publishEvent(new MagicHappenedEvent(magician));
    }
}

class SignupThen extends Then<SignupThen, SignupGivenWhen> {

    SignupThen customerWaitingForMagic() {
        return executionExists()
                .waitingIn("waitForMagicToHappen");
    }

    SignupThen signupCompletedFor(String id, String email, String magician) {
        return workflowFinished(WorkflowStatus.COMPLETED)
                .stepsPassed("activateUser", "sendWelcomeEmail", "waitForMagicToHappen", "modifyPayload")
                .payloadContains(Map.of(
                        "id", id,
                        "email", email,
                        "status", "vip",
                        "magician", magician
                ));
    }
}
// end::custom-phases[]
