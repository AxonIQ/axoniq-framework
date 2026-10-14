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
