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
package io.axoniq.workflow.itest;

import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.test.AbstractWorkflowTestBase;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.List;
import java.util.function.Function;

import static io.axoniq.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.workflow.runtime.association.Associations.associate;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Releasing a segment reports it quiet only once the bodies it interrupted have unwound.
 * <p>
 * The node handing a segment over must stop working on that segment's instances before another node resumes them from
 * their persisted state. Reporting the release while a body is still running lets both nodes drive the same instance
 * for as long as it takes the first one to notice the interrupt.
 */
class SegmentReleaseDrainTest extends AbstractWorkflowTestBase<SimpleWorkflowContext> {

    private static final String WORKFLOW_ID = "user-parked";

    SegmentReleaseDrainTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d.autodetected(c -> new ParkedWorkflow());
    }

    @Test
    void releasingASegmentReportsQuietOnlyAfterTheParkedBodyUnwound() {
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

        CompletableFuture<Void> drained = parked.interrupt();

        assertThat(drained)
                .as("""
                    Interrupting '%s' must report back once its parked body unwound. A future that never completes \
                    holds the release of every segment owning a parked instance until the processor cuts the wait at \
                    its claim extension threshold, and one completing early reports this node quiet while the body \
                    still runs.""", WORKFLOW_ID)
                .succeedsWithin(Duration.ofSeconds(10));
    }

    public static class ParkedWorkflow {

        @Workflow(
                workflowName = "ParkedWorkflow",
                workflowNamespace = "io.axoniq.dsl.parked",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(@Nonnull SimpleWorkflowContext ctx) {
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
