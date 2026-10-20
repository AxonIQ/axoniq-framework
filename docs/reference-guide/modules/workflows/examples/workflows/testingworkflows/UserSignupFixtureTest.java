/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.testingworkflows;

// tag::user-signup-fixture-test[]

import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.test.fixture.GivenWhen;
import io.axoniq.framework.workflow.runtime.test.fixture.Then;
import io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestFixture;
import org.junit.jupiter.api.*;

class UserSignupFixtureTest {

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

    @Test
    void vip_user_reaches_activation_step() {
        fixture.given()
               .noExecution();

        fixture.when()
               .publishEvent(new RegistrationReceivedEvent("2", "piggy@muppets.biz", "vip"))
               .execute("createUser");

        fixture.then()
               .executionExists()
               .waitingIn("activateUser");
    }
}
// end::user-signup-fixture-test[]
