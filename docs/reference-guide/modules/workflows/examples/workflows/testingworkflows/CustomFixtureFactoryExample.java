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
