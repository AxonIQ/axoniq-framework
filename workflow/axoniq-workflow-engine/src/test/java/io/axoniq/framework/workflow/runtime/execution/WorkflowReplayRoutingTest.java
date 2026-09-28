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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;

import java.util.Map;

import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Walk-through tests for {@link WorkflowConfigurationRegistry#resolveDefinitionForReplay}. One test per row in the
 * routing table below, in the same order so a reader can pattern-match a routing decision in production logs to the
 * scenario that triggers it.
 *
 * <p>{@code start} is the engine's default pick before routing thinks about it (typically the
 * highest registered version for this workflow name). {@code target} is what routing chose instead after looking at
 * {@code state}.
 *
 * <pre>
 * | state          | registered          | start  | → target | tier
 * |----------------|---------------------|--------|----------|------------------------------------------------------------------
 * | 1.0.0          | [1.0.0]             | 1.0.0  | 1.0.0    | exact-match-start-config  - state == start, use it.
 * | 1.0.0          | [1.0.0, 2.0.0]      | 2.0.0  | 1.0.0    | exact-match-sibling       - registry has a sibling exactly at state, use it.
 * | 1.0.1          | [1.0.0, 2.0.0]      | 2.0.0  | 1.0.0    | closest-sibling           - no exact match; pick the closest registered version below state.
 * | 0.0.1          | [0.0.2]             | 0.0.2  | 0.0.2    | closest-higher-sibling    - nothing registered ≤ state; pick the closest above.
 * | 0.0.1          | [0.0.2, 0.0.5]      | 0.0.5  | 0.0.2    | closest-higher-sibling    - pick the lowest above state, not the highest.
 * | 1.5.0          | [2.0.0]             | 2.0.0  | 2.0.0    | closest-higher-sibling    - closest above wins even across a major boundary.
 * | 0.0.5          | [0.0.1]             | 0.0.1  | 0.0.1    | closest-sibling           - one definition registered, state drifted past it via migrateVersion.
 * | not-a-semver   | [1.0.0]             | 1.0.0  | 1.0.0    | legacy-fallback           - state isn't parseable semver; fall back to start.
 * </pre>
 *
 * @author Stefan Dragisic
 */
class WorkflowReplayRoutingTest {

    private static final String WORKFLOW = "OrderWorkflow";
    private static final QualifiedName EVENT = new QualifiedName("com.example.OrderPlaced");

    private SimpleWorkflowConfigurationRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SimpleWorkflowConfigurationRegistry();
    }

    // ----- Row 1 -------------------------------------------------------------------------------

    @Test
    @DisplayName("exact-match-start-config - state == start, use it")
    void row1ExactMatchSpawnConfig() {
        var spawn = new TestConfig(WORKFLOW, "1.0.0");
        registry.register(EVENT, spawn);

        var picked = registry.resolveDefinitionForReplay(WORKFLOW, "wf-1", "1.0.0", spawn);
        assertThat(picked.workflowVersion()).isEqualTo("1.0.0");
    }

    // ----- Row 2 -------------------------------------------------------------------------------

    @Test
    @DisplayName("exact-match-sibling - registry has a sibling exactly at state, use it")
    void row2ExactMatchSibling() {
        var v1 = new TestConfig(WORKFLOW, "1.0.0");
        var v2 = new TestConfig(WORKFLOW, "2.0.0");
        registry.register(EVENT, v1);
        registry.register(EVENT, v2);

        var picked = registry.resolveDefinitionForReplay(WORKFLOW, "wf-1", "1.0.0", v2);
        assertThat(picked.workflowVersion()).isEqualTo("1.0.0");
    }

    // ----- Row 3 -------------------------------------------------------------------------------

    @Test
    @DisplayName("closest-sibling - no exact match; pick the closest registered version below state")
    void row3ClosestSiblingPicksHighestBelowState() {
        var v1 = new TestConfig(WORKFLOW, "1.0.0");
        var v2 = new TestConfig(WORKFLOW, "2.0.0");
        registry.register(EVENT, v1);
        registry.register(EVENT, v2);

        var picked = registry.resolveDefinitionForReplay(WORKFLOW, "wf-1", "1.0.1", v2);
        assertThat(picked.workflowVersion()).isEqualTo("1.0.0");
    }

    // ----- Row 4 -------------------------------------------------------------------------------

    @Test
    @DisplayName("closest-higher-sibling - nothing registered ≤ state; pick the closest above")
    void row4ClosestHigherSiblingPicksLowestAboveState() {
        var v002 = new TestConfig(WORKFLOW, "0.0.2");
        registry.register(EVENT, v002);

        var picked = registry.resolveDefinitionForReplay(WORKFLOW, "wf-1", "0.0.1", v002);
        assertThat(picked.workflowVersion()).isEqualTo("0.0.2");
    }

    // ----- Row 5 -------------------------------------------------------------------------------

    @Test
    @DisplayName("closest-higher-sibling - pick the lowest above state, not the highest")
    void row5ClosestHigherSiblingPicksLowestNotHighest() {
        var v002 = new TestConfig(WORKFLOW, "0.0.2");
        var v005 = new TestConfig(WORKFLOW, "0.0.5");
        registry.register(EVENT, v002);
        registry.register(EVENT, v005);

        var picked = registry.resolveDefinitionForReplay(WORKFLOW, "wf-1", "0.0.1", v005);
        assertThat(picked.workflowVersion()).isEqualTo("0.0.2");
    }

    // ----- Row 6 -------------------------------------------------------------------------------

    @Test
    @DisplayName("closest-higher-sibling - closest above wins even across a major boundary")
    void row6ClosestHigherSiblingCrossesMajorBoundary() {
        var v2 = new TestConfig(WORKFLOW, "2.0.0");
        registry.register(EVENT, v2);

        var picked = registry.resolveDefinitionForReplay(WORKFLOW, "wf-1", "1.5.0", v2);
        assertThat(picked.workflowVersion()).isEqualTo("2.0.0");
    }

    // ----- Row 7 -------------------------------------------------------------------------------

    @Test
    @DisplayName("closest-sibling - one definition registered, state drifted past it via migrateVersion")
    void row7ClosestSiblingStateDriftedPastOnlyRegistered() {
        var v001 = new TestConfig(WORKFLOW, "0.0.1");
        registry.register(EVENT, v001);

        var picked = registry.resolveDefinitionForReplay(WORKFLOW, "wf-1", "0.0.5", v001);
        assertThat(picked.workflowVersion()).isEqualTo("0.0.1");
    }

    // ----- Row 8 -------------------------------------------------------------------------------

    @Test
    @DisplayName("legacy-fallback - state isn't parseable semver; fall back to start")
    void row8LegacyFallbackWhenStateIsUnparseable() {
        var start = new TestConfig(WORKFLOW, "1.0.0");
        registry.register(EVENT, start);

        var picked = registry.resolveDefinitionForReplay(WORKFLOW, "wf-1", "not-a-semver", start);
        assertThat(picked.workflowVersion()).isEqualTo("1.0.0");
    }

    // ----- Additional safety-net coverage (not in the table) ----------------------------------

    @Test
    @DisplayName("no-match-fallback - nothing registered for this workflow name; use start config + WARN")
    void noMatchFallbackWhenNothingRegisteredForThisName() {
        // Defensive: caller passes a start config not in the registry. In normal operation this
        // does not happen (the start config is always one of the registered configs), but the
        // routing logic must still return something usable.
        registry.register(EVENT, new TestConfig("PaymentWorkflow", "1.0.0"));

        var start = new TestConfig(WORKFLOW, "5.0.0");
        var picked = registry.resolveDefinitionForReplay(WORKFLOW, "wf-1", "0.0.1", start);
        assertThat(picked.workflowVersion()).isEqualTo("5.0.0");
    }

    /**
     * Minimal {@link WorkflowConfiguration} carrying just a name + version for routing tests.
     */
    private static final class TestConfig implements WorkflowConfiguration<WorkflowContext> {

        private final String name;
        private final String version;

        TestConfig(String name, String version) {
            this.name = name;
            this.version = version;
        }

        @Override
        public String workflowName() {
            return name;
        }

        @Override
        public String workflowVersion() {
            return version;
        }

        @Override
        public Class<WorkflowContext> getWorkflowContextType() {
            return WorkflowContext.class;
        }

        @Override
        public WorkflowDefinition<WorkflowContext> workflowDefinition() {
            return ctx -> { /* no-op for routing-only tests */ };
        }

        @Override
        public WorkflowContextFactory<WorkflowContext> workflowContextFactory() {
            return (initialPayload, workflowId, processingContext, workflowConfiguration) -> {
                throw new UnsupportedOperationException("Routing-test config does not create contexts");
            };
        }

        @Override
        public WorkflowExecutionFactory workflowExecutionFactory() {
            return new WorkflowContextAdoptingExecutionFactory<>(getWorkflowContextType());
        }

        @Override
        public WorkflowIdProvider workflowIdProvider() {
            return new MessageWorkflowIdProvider();
        }

        @Override
        public Map<WorkflowStatus, WorkflowStatusChangeListener> workflowStatusChangeListeners() {
            return Map.of();
        }

        @Override
        public EventNameCustomizer eventNameCustomizer() {
            return defaults();
        }
    }
}
