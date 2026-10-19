/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.annotation.Workflow;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Stopping a workflow execution unwinds a parked workflow body without publishing cancellation events.
 * <p>
 * The node handing a segment over must stop working on that segment's instances before another node resumes them from
 * their persisted state.
 */
class SegmentReleaseDrainTest extends AbstractWorkflowIntegrationTestBase<SimpleWorkflowContext> {

    private static final String WORKFLOW_ID = "user-parked";

    SegmentReleaseDrainTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d.autodetected(c -> new ParkedWorkflow());
    }

    @Test
    void stoppingAParkedWorkflowUnwindsItsBody() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(200, new RegistrationReceivedEvent(WORKFLOW_ID, "parked@test.com", "vip"))
        ));
        delayedPublisher.start();

        // The body reached its await and is parked there, holding the instance running.
        testDriver.historyMatches(h -> h.state().workflowStepNames().contains("parked"));
        var parked = workflowEngine.workflowExecutions()
                                   .stream()
                                   .filter(execution -> execution.workflowId().equals(WORKFLOW_ID))
                                   .findFirst()
                                   .orElseThrow();

        parked.stopForShutdown();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                                                                     assertThat(parked.isRunning()).isFalse());
    }

    public static class ParkedWorkflow {

        @Workflow(
                workflowName = "ParkedWorkflow",
                workflowNamespace = "io.axoniq.dsl.parked",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            // Waits for an event that is never published, so the body stays parked until it is interrupted.
            var id = String.valueOf(ctx.workflowPayload().get("id"));
            ctx.awaitEvent("parked",
                           PaymentReceivedEvent.class,
                           associate(payloadProperty("id"), equalsTo(id)),
                           step -> step);
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }

    @Event(namespace = "my.custom", name = "PaymentReceived")
    public record PaymentReceivedEvent(String id) {

    }
}
