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

class TimeoutFixtureTest {

    WorkflowTestFixture<GivenWhen.Phase, Then.Phase> fixture;

    @BeforeEach
    void setUp() {
        WorkflowModule<SimpleWorkflowContext> module = WorkflowModule
                .defaults("ConfirmationTimeout", SimpleWorkflowContext.class)
                .definition(d -> d.autodetected(c -> new ConfirmationTimeoutWorkflow()));

        fixture = WorkflowTestFixture.of(module);
    }

    @Test
    void confirmation_wait_times_out() {
        // tag::timeout-fixture-test[]
        fixture.when()
               .publishEvent(new PaymentRequested("payment-1"))
               .executionExists()
               .timePasses(Duration.ofMinutes(5).plusSeconds(1));

        fixture.then()
               .step("waitForConfirmation", StepStatus.TIMED_OUT)
               .waitingIn("handleTimeout");

        fixture.when()
               .executeReturning("handleTimeout", Map.of("handledBy", "test"));

        fixture.then()
               .workflowFinished(WorkflowStatus.COMPLETED);
        // end::timeout-fixture-test[]
    }
}
