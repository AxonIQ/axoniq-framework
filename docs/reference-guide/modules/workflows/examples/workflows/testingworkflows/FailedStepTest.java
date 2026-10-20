/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.testingworkflows;

import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.test.fixture.GivenWhen;
import io.axoniq.framework.workflow.runtime.test.fixture.Then;
import io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestFixture;
import org.junit.jupiter.api.*;

class FailedStepTest {

    WorkflowTestFixture<GivenWhen.Phase, Then.Phase> fixture;

    @BeforeEach
    void setUp() {
        WorkflowModule<SimpleWorkflowContext> module = WorkflowModule
                .defaults("UserSignup", SimpleWorkflowContext.class)
                .definition(d -> d.autodetected(c -> new UserSignupWorkflow()));

        fixture = WorkflowTestFixture.of(module);
    }

    @Test
    void workflow_fails_when_step_fails() {
        // tag::failed-step-test[]
        fixture.given()
               .publishEvent(new RegistrationReceivedEvent("2", "piggy@muppets.biz", "vip"))
               .execute("createUser");

        fixture.when()
               .executeFailing("activateUser", new StepFailedException("activation service down"));

        fixture.then()
               .workflowFinished(WorkflowStatus.FAILED)
               .step("activateUser", StepStatus.FAILED)
               .noStep("sendWelcomeEmail");
        // end::failed-step-test[]
    }
}
