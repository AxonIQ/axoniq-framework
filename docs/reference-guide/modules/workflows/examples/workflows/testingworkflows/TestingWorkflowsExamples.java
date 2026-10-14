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
import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.test.fixture.GivenWhen;
import io.axoniq.framework.workflow.runtime.test.fixture.Then;
import io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestFixture;

import java.time.Duration;
import java.util.Map;

public class TestingWorkflowsExamples {

    public void bddOverview(WorkflowTestFixture<GivenWhen.Phase, Then.Phase> fixture) {
        // tag::bdd-overview[]
        fixture.given()
               .publishEvent(new RegistrationReceivedEvent("2", "piggy@muppets.biz", "vip"))
               .execute("createUser");

        fixture.when()
               .executeReturning("activateUser", Map.of());

        fixture.then()
               .executionExists()
               .waitingIn("sendWelcomeEmail");
        // end::bdd-overview[]
    }

    public void customizeOverload() {
        // tag::customize-overload[]
        WorkflowModule<SimpleWorkflowContext> module = WorkflowModule
                .defaults("UserSignup", SimpleWorkflowContext.class)
                .definition(d -> d.autodetected(c -> new UserSignupWorkflow()));

        WorkflowTestFixture<GivenWhen.Phase, Then.Phase> fixture =
                WorkflowTestFixture.of(
                        module,
                        configurer -> configurer.componentRegistry(registry -> registry.registerComponent(
                                AuditSink.class,
                                c -> new InMemoryAuditSink()
                        ))
                );
        // end::customize-overload[]
    }

    public void phaseMarkers(WorkflowTestFixture<GivenWhen.Phase, Then.Phase> fixture, Object startEvent,
                             Object domainEvent) {
        // tag::phase-markers[]
        fixture.given()
               .publishEvent(startEvent)
               .execute("prepare");

        fixture.when()
               .publishEvent(domainEvent);

        fixture.then()
               .workflowFinished(WorkflowStatus.COMPLETED);
        // end::phase-markers[]
    }

    public void publishEventBasic(WorkflowTestFixture<GivenWhen.Phase, Then.Phase> fixture) {
        // tag::publish-event-basic[]
        fixture.when()
               .publishEvent(new RegistrationReceivedEvent("2", "piggy@muppets.biz", "vip"));
        // end::publish-event-basic[]
    }

    public void executeRelease(WorkflowTestFixture<GivenWhen.Phase, Then.Phase> fixture) {
        // tag::execute-release[]
        fixture.when()
               .execute("createUser");
        // end::execute-release[]
    }

    public void executeReturning(WorkflowTestFixture<GivenWhen.Phase, Then.Phase> fixture) {
        // tag::execute-returning[]
        fixture.when()
               .executeReturning("activateUser", Map.of("activated", true));
        // end::execute-returning[]
    }

    public void executeCustomProcessor(WorkflowTestFixture<GivenWhen.Phase, Then.Phase> fixture) {
        // tag::execute-custom-processor[]
        fixture.when()
               .execute(
                       "reserveCredit",
                       (processingContext, inputPayload) -> Map.of(
                               "reservationId", "res-123",
                               "amount", inputPayload.get("amount")
                       )
               );
        // end::execute-custom-processor[]
    }

    public void executeFailing(WorkflowTestFixture<GivenWhen.Phase, Then.Phase> fixture) {
        // tag::execute-failing[]
        fixture.when()
               .executeFailing(
                       "reserveCredit",
                       new StepFailedException("credit denied")
               );
        // end::execute-failing[]
    }

    public void advancingTime(WorkflowTestFixture<GivenWhen.Phase, Then.Phase> fixture) {
        // tag::advancing-time[]
        fixture.given()
               .publishEvent(new RegistrationReceivedEvent("2", "piggy@muppets.biz", "vip"))
               .execute("createUser")
               .executeReturning("activateUser", Map.of())
               .executeReturning("sendWelcomeEmail", Map.of());

        fixture.when()
               .timePasses(Duration.ofSeconds(1));

        fixture.then()
               .waitingIn("waitForMagicToHappen");
        // end::advancing-time[]
    }

    public void businessLevelFlow(WorkflowTestFixture<SignupGivenWhen, SignupThen> fixture) {
        // tag::business-level-flow[]
        fixture.given()
               .vipRegistrationReceived("2", "piggy@muppets.biz")
               .customerRegistrationCompleted()
               .timePasses(Duration.ofSeconds(1));

        fixture.then()
               .customerWaitingForMagic();

        fixture.when()
               .magicHappens("Merlin");

        fixture.then()
               .signupCompletedFor("2", "piggy@muppets.biz", "Merlin");
        // end::business-level-flow[]
    }

    public void andUsage(WorkflowTestFixture<SignupGivenWhen, SignupThen> fixture) {
        // tag::and-usage[]
        fixture.then()
               .customerWaitingForMagic()
               .and()
               .when()
               .magicHappens("Merlin");
        // end::and-usage[]
    }
}
