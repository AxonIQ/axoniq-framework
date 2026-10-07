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
