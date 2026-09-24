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
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for multi-version routing: registers two workflow definitions sharing the same
 * {@code workflowName}/{@code startOnEvent}/{@code idProperty} triple but differing by version. Verifies that a fresh
 * start picks the highest version, and that — after a cross-version collision triggers disambiguation — both versions
 * can coexist for the same domain key, each running its own body.
 *
 * @author Stefan Dragisic
 */
class MultiVersionRoutingWorkflowTest extends AbstractWorkflowIntegrationTestBase<SimpleWorkflowContext> {

    private static final String WORKFLOW_NAME = "MultiVersionOrder";
    private static final String NAMESPACE = "io.axoniq.dsl.multiversion";

    private V1Body v1;
    private V2Body v2;

    public MultiVersionRoutingWorkflowTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowConfigurer> configure() {
        this.v1 = new V1Body();
        this.v2 = new V2Body();
        return super.configure();
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> v1);
    }

    @Override
    protected List<Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>>> getAdditionalDefinitions() {
        return List.of(d -> d
                .autodetected(c -> v2)
        );
    }

    @Test
    void freshStartPicksHighestVersionAndRunsV2Body() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new OrderPlacedEvent("order-1", "alice"))
        ));
        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus() == WorkflowStatus.COMPLETED);

        var state = testDriver.testingState().state();
        assertThat(state.workflowDefinitionId().version())
                .as("Fresh start should pick the highest registered version")
                .isEqualTo("2.0.0");
        testDriver.testingState().hasSteps("v2-only-step");
        testDriver.testingState().noStep("v1-only-step");
        assertThat(v1.executed).as("v1 body must not have run").isFalse();
        assertThat(v2.executed).as("v2 body must have run").isTrue();
    }

    /**
     * Smart-routing regression: when a workflow bumps to a version that has no exact sibling registered (e.g. v2 starts
     * at "2.0.0" then ctx.migrateVersion("...", "2.0.1") bumps it to "2.0.1"), the dispatcher must route to the CLOSEST
     * registered version &lt;= the new state version — v2.0.0 here, NOT jump across the major to v1.0.0. Without
     * {@code findClosestRegisteredVersion} the v2 workflow would cross-jump or fail.
     */
    @Test
    void bumpedVersionRoutesToClosestRegisteredDefinition() {
        v2.bumpInsideBody = true; // make V2Body call ctx.migrateVersion(..., "2.0.1") before its step

        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new OrderPlacedEvent("order-bump", "alice"))
        ));
        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus() == WorkflowStatus.COMPLETED);

        var state = testDriver.testingState().state();
        assertThat(state.workflowDefinitionId().version())
                .as("ctx.migrateVersion bumped state to 2.0.1")
                .isEqualTo("2.0.1");
        testDriver.testingState().hasSteps("v2-only-step");
        testDriver.testingState().noStep("v1-only-step");
        assertThat(v1.executed).as("v1 body must not have run").isFalse();
    }

    /**
     * Single-version test body bound at registration: runs a v1-only step and finishes.
     */
    public static class V1Body {

        volatile boolean executed = false;

        @Workflow(
                workflowName = WORKFLOW_NAME,
                workflowNamespace = NAMESPACE,
                workflowVersion = "1.0.0",
                idProperty = "orderId",
                startOnEventClass = OrderPlacedEvent.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            executed = true;
            ctx.awaitExecute("v1-only-step", Boolean.class, () -> true);
        }
    }

    public static class V2Body {

        volatile boolean executed = false;
        volatile boolean bumpInsideBody = false;

        @Workflow(
                workflowName = WORKFLOW_NAME,
                workflowNamespace = NAMESPACE,
                workflowVersion = "2.0.0",
                idProperty = "orderId",
                startOnEventClass = OrderPlacedEvent.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            executed = true;
            if (bumpInsideBody) {
                ctx.migrateVersion("v2-patch", "2.0.1");
            }
            ctx.awaitExecute("v2-only-step", Boolean.class, () -> true);
        }
    }

    @Event(namespace = "my.custom", name = "OrderPlaced")
    public record OrderPlacedEvent(String orderId, String customerId) {

    }
}
