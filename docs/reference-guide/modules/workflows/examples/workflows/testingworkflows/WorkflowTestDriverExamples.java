/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.testingworkflows;

import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.test.configuration.WorkflowTestSteppingModeEnhancer;
import io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestDriver;

import java.util.function.UnaryOperator;

public class WorkflowTestDriverExamples {

    public void stepperExample() {
        WorkflowModule<SimpleWorkflowContext> module = WorkflowModule
                .defaults("TestModule", SimpleWorkflowContext.class)
                .definition(d -> d.autodetected(c -> new UserSignupWorkflow()));

        // tag::driver-stepper-example[]
        WorkflowTestDriver driver = WorkflowTestDriver.stepper(module, false, UnaryOperator.identity());

        try {
            driver.publishEvent(new RegistrationReceivedEvent("2", "piggy@muppets.biz", "vip"));
            driver.executionExists();
            driver.testingState().waitingIn("createUser");
            driver.executeStep("createUser");
            driver.testingState().waitingIn("activateUser");
        } finally {
            driver.shutdown();
        }
        // end::driver-stepper-example[]
    }

    public void liveExample(WorkflowModule<SimpleWorkflowContext> module) {
        // tag::driver-live-example[]
        WorkflowTestDriver driver = WorkflowTestDriver.live(module, UnaryOperator.identity());
        // end::driver-live-example[]
    }

    public void steppingEnhancerExample(WorkflowModule<SimpleWorkflowContext> module) {
        // tag::stepping-enhancer-example[]
        WorkflowConfigurer configurer =
                WorkflowConfigurer.create()
                                  .componentRegistry(registry -> new WorkflowTestSteppingModeEnhancer().enhance(registry))
                                  .registerWorkflowModule(module);
        // end::stepping-enhancer-example[]
    }
}
