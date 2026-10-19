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
import io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestFixture;

import java.util.function.UnaryOperator;

public class CustomFixtureFactoryExample {

    public void create() {
        // tag::custom-fixture-factory[]
        WorkflowTestFixture<SignupGivenWhen, SignupThen> fixture =
                WorkflowTestFixture.of(
                        WorkflowModule.defaults("UserSignup", SimpleWorkflowContext.class)
                                      .definition(d -> d.autodetected(c -> new UserSignupWorkflow())),
                        UnaryOperator.identity(),
                        new SignupGivenWhen(),
                        new SignupThen()
                );
        // end::custom-fixture-factory[]
    }
}
