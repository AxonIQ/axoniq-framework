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

import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.test.fixture.GivenWhen;
import io.axoniq.framework.workflow.runtime.test.fixture.Then;
import io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestFixture;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Map;

class CompleteScenarioTest {

    WorkflowTestFixture<GivenWhen.Phase, Then.Phase> fixture;

    @BeforeEach
    void setUp() {
        WorkflowModule<SimpleWorkflowContext> module = WorkflowModule
                .defaults("UserSignup", SimpleWorkflowContext.class)
                .definition(d -> d.autodetected(c -> new UserSignupWorkflow()));

        fixture = WorkflowTestFixture.of(module);
    }

    @AfterEach
    void tearDown() {
        fixture.then().stop();
    }

    // tag::complete-scenario[]
    @Test
    void vip_registration_completes_after_magic_event() {
        fixture.given()
               .publishEvent(new RegistrationReceivedEvent("2", "piggy@muppets.biz", "vip"))
               .executionExists()
               .execute("createUser")
               .executeReturning("activateUser", Map.of())
               .executeReturning("sendWelcomeEmail", Map.of())
               .timePasses(Duration.ofSeconds(1))
               .then()
               .waitingIn("waitForMagicToHappen");

        fixture.when()
               .publishEvent(new MagicHappenedEvent("Merlin"));

        fixture.then()
               .workflowFinished(WorkflowStatus.COMPLETED)
               .stepsPassed("activateUser", "sendWelcomeEmail", "waitForMagicToHappen", "modifyPayload")
               .step("activateUser", StepStatus.COMPLETED)
               .payloadEquals(Map.of(
                       "magician", "Merlin",
                       "email", "piggy@muppets.biz",
                       "status", "vip",
                       "id", "2"
               ));
    }
    // end::complete-scenario[]
}
