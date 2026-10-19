/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.testingworkflows;

import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.test.fixture.GivenWhen;
import io.axoniq.framework.workflow.runtime.test.fixture.Then;
import io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestFixture;
import org.junit.jupiter.api.*;

class MultipleInstancesTest {

    WorkflowTestFixture<GivenWhen.Phase, Then.Phase> fixture;

    @BeforeEach
    void setUp() {
        WorkflowModule<SimpleWorkflowContext> module = WorkflowModule
                .defaults("UserSignup", SimpleWorkflowContext.class)
                .definition(d -> d.autodetected(c -> new UserSignupWorkflow()));

        fixture = WorkflowTestFixture.of(module);
    }

    @Test
    void several_instances_are_selected_individually() {
        // tag::multiple-instances-test[]
        fixture.given()
               .publishEvent(new RegistrationReceivedEvent("2", "piggy@muppets.biz", "vip"))
               .publishEvent(new RegistrationReceivedEvent("3", "kermit@muppets.biz", "vip"));

        fixture.then()
               .executionExists(execution -> "3".equals(execution.state().payload().get("id")))
               .waitingIn("createUser");
        // end::multiple-instances-test[]
    }
}
