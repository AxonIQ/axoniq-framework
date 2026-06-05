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
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.OrderPlacedEvent;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.history.api.WorkflowHistory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end test for multi-version routing: registers two workflow definitions sharing the same
 * {@code workflowName}/{@code startOnEvent}/{@code idProperty} triple but differing by version. Verifies
 * that a fresh start picks the highest version, and that — after a cross-version collision triggers
 * disambiguation — both versions can coexist for the same domain key, each running its own body.
 *
 * @author Stefan Dragisic
 */
class MultiVersionRoutingDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    private static final String WORKFLOW_NAME = "MultiVersionOrder";
    private static final String NAMESPACE = "io.axoniq.dsl.multiversion";

    private final V1Body v1 = new V1Body();
    private final V2Body v2 = new V2Body();

    public MultiVersionRoutingDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .declarative(c -> v1::execute)
                .workflowName(WORKFLOW_NAME)
                .on(EventConditions.fromType(OrderPlacedEvent.class))
                .customized((c, w) -> w
                        .workflowVersion("1.0.0")
                        .eventNameCustomizer(namespace(NAMESPACE).workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "orderId", id -> "order-" + id))
                );
    }

    @Override
    protected List<Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>>> getAdditionalDefinitions() {
        return List.of(d -> d
                .declarative(c -> v2::execute)
                .workflowName(WORKFLOW_NAME)
                .on(EventConditions.fromType(OrderPlacedEvent.class))
                .customized((c, w) -> w
                        .workflowVersion("2.0.0")
                        .eventNameCustomizer(namespace(NAMESPACE).workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "orderId", id -> "order-" + id))
                )
        );
    }

    @Test
    void freshStart_picksHighestVersion_andRunsV2Body() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new OrderPlacedEvent("order-1", "alice"))
        ));
        delayedPublisher.start();

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).hasSize(1);
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(e -> e.state().workflowStatus().isTerminal());
        });

        var history = workflowHistoryRepository.findAll().iterator().next();
        var state = history.state();
        assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(state.workflowDefinitionVersion())
                .as("Fresh start should pick the highest registered version")
                .isEqualTo("2.0.0");
        assertThat(state.workflowStepNames())
                .as("Should run the v2 body which has a v2-only step")
                .contains("v2-only-step");
        assertThat(state.workflowStepNames())
                .as("Should NOT run the v1-only step")
                .doesNotContain("v1-only-step");
        assertThat(v1.executed).as("v1 body must not have run").isFalse();
        assertThat(v2.executed).as("v2 body must have run").isTrue();
    }

    /**
     * Smart-routing regression: when a workflow bumps to a version that has no exact sibling registered
     * (e.g. v2 starts at "2.0.0" then ctx.migrateVersion("...", "2.0.1") bumps it to "2.0.1"), the dispatcher
     * must route to the CLOSEST registered version &lt;= the new state version — v2.0.0 here, NOT jump
     * across the major to v1.0.0. Without {@code findClosestRegisteredVersion} the v2 workflow would
     * cross-jump or fail.
     */
    @Test
    void bumpedVersion_routesToClosestRegisteredDefinition() {
        v2.bumpInsideBody = true; // make V2Body call ctx.migrateVersion(..., "2.0.1") before its step

        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new OrderPlacedEvent("order-bump", "alice"))
        ));
        delayedPublisher.start();

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).hasSize(1);
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(e -> e.state().workflowStatus().isTerminal());
        });

        var state = workflowHistoryRepository.findAll().iterator().next().state();
        assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(state.workflowDefinitionVersion())
                .as("ctx.migrateVersion bumped state to 2.0.1")
                .isEqualTo("2.0.1");
        assertThat(state.workflowStepNames())
                .as("v2 body should run the v2-only step even after the bump — closest registered <= 2.0.1 is 2.0.0")
                .contains("v2-only-step");
        assertThat(state.workflowStepNames())
                .as("must NOT cross over to v1 code after the bump")
                .doesNotContain("v1-only-step");
        assertThat(v1.executed).as("v1 body must not have run").isFalse();
    }

    /**
     * Single-version test body bound at registration: runs a v1-only step and finishes.
     */
    static class V1Body {
        volatile boolean executed = false;

        void execute(SimpleWorkflowContext ctx) {
            executed = true;
            ctx.awaitExecute("v1-only-step", Boolean.class, () -> true);
        }
    }

    static class V2Body {
        volatile boolean executed = false;
        volatile boolean bumpInsideBody = false;

        void execute(SimpleWorkflowContext ctx) {
            executed = true;
            if (bumpInsideBody) {
                ctx.migrateVersion("v2-patch", "2.0.1");
            }
            ctx.awaitExecute("v2-only-step", Boolean.class, () -> true);
        }
    }
}
