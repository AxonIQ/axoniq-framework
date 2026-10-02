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
package io.axoniq.framework.workflow.runtime.test.fixture;

import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import org.axonframework.common.configuration.ComponentBuilder;

import java.util.function.UnaryOperator;

/**
 * Staged fixture for BDD-style workflow runtime tests.
 *
 * <p>This fixture wraps a {@link WorkflowTestDriver} in a small phase-oriented API so tests can read as
 * <em>given / when / then</em> scenarios while still exercising the real workflow runtime. The fixture starts the
 * workflow module in stepping mode, publishes real events, records normal workflow history, and keeps the selected
 * execution or history entry shared across all phases.</p>
 *
 * <p>The phase model is intentionally small:</p>
 * <ul>
 *     <li>{@link #given()} returns the action phase for arranging workflow state before the behavior under test</li>
 *     <li>{@link #when()} returns the same action phase, but named for the stimulus or transition being tested</li>
 *     <li>{@link #then()} returns the assertion phase for verifying workflow state, steps, history, or payload</li>
 * </ul>
 *
 * <p>All phases are bound to the same underlying workflow runtime. Switching between them does not reset the fixture or
 * start a new workflow test driver; it only changes which fluent operations are exposed next.</p>
 *
 * <p>Failures are intentionally surfaced differently per phase. Action-phase methods reached through {@link #given()}
 * and {@link #when()} use {@link WorkflowTestFixtureException} for fixture-driving failures such as missing executions
 * or steps that never reach the expected terminal state. Assertion-phase methods reached through {@link #then()} fail
 * with plain {@link AssertionError AssertionErrors}, so they behave like conventional test assertions.</p>
 *
 * @param <ACTION> type of the action phase used for {@code given()} and {@code when()}
 * @param <ASSERT> type of the assertion phase used for {@code then()}
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public interface WorkflowTestFixture<ACTION extends GivenWhen<ACTION, ASSERT>, ASSERT extends Then<ASSERT, ACTION>> {

    /**
     * Builds an autodetected {@link WorkflowModule} registered under {@link WorkflowTestServices#DEFAULT_MODULE_NAME},
     * ready to be passed into {@link #of(WorkflowModule)} or {@link #of(WorkflowModule, UnaryOperator)}.
     *
     * @param contextType                   type of the workflow context
     * @param workflowContextFactoryBuilder builder of the {@link WorkflowContextFactory} for the workflow context
     * @param workflowInstanceBuilder       builder of the workflow instance to autodetect handlers on
     * @param <T>                           type of the workflow context
     * @return workflow module ready for use in a {@code WorkflowTestFixture}
     */
    static <T extends WorkflowContext> WorkflowModule<T> workflowModule(
            Class<T> contextType,
            ComponentBuilder<WorkflowContextFactory<T>> workflowContextFactoryBuilder,
            ComponentBuilder<Object> workflowInstanceBuilder
    ) {
        return WorkflowModule.defaults(WorkflowTestServices.DEFAULT_MODULE_NAME, contextType)
                             .definition(d -> d.autodetected(workflowInstanceBuilder))
                             .contextFactory(workflowContextFactoryBuilder);
    }

    /**
     * Creates a fixture with the default action and assertion phases.
     *
     * @param workflowModule workflow module to test
     * @param <T>            type of the workflow context
     * @return fixture backed by the supplied workflow module
     */
    static <T extends WorkflowContext> WorkflowTestFixture<GivenWhen.Phase, Then.Phase> of(
            WorkflowModule<T> workflowModule
    ) {
        return new DefaultWorkflowTestFixture<>(workflowModule, UnaryOperator.identity(),
                                                new GivenWhen.Phase(), new Then.Phase());
    }

    /**
     * Creates a fixture with the default action and assertion phases and a custom configuration step.
     *
     * @param workflowModule workflow module to test
     * @param customize      customizer of the workflow configuration
     * @param <T>            type of the workflow context
     * @return fixture backed by the supplied workflow module and customization
     */
    static <T extends WorkflowContext> WorkflowTestFixture<GivenWhen.Phase, Then.Phase> of(
            WorkflowModule<T> workflowModule,
            UnaryOperator<WorkflowConfigurer> customize
    ) {
        return new DefaultWorkflowTestFixture<>(workflowModule, customize,
                                                new GivenWhen.Phase(), new Then.Phase());
    }

    /**
     * Creates a fixture with caller-supplied action and assertion phases.
     *
     * <p>Use this overload to introduce domain-specific phase methods while keeping the same underlying fixture
     * behavior.</p>
     *
     * @param workflowModule workflow module to test
     * @param customize      customizer of the workflow configuration
     * @param givenWhenPhase phase instance used for {@code given()} and {@code when()}
     * @param thenPhase      phase instance used for {@code then()}
     * @param <T>            type of the workflow context
     * @param <ACTION>       type of the action phase
     * @param <ASSERT>       type of the assertion phase
     * @return fixture backed by the supplied workflow module and custom phases
     */
    static <T extends WorkflowContext,
            ACTION extends GivenWhen<ACTION, ASSERT>,
            ASSERT extends Then<ASSERT, ACTION>
            > WorkflowTestFixture<ACTION, ASSERT> of(
            WorkflowModule<T> workflowModule,
            UnaryOperator<WorkflowConfigurer> customize,
            ACTION givenWhenPhase,
            ASSERT thenPhase
    ) {
        return new DefaultWorkflowTestFixture<>(workflowModule, customize, givenWhenPhase, thenPhase);
    }

    /**
     * Returns the action phase in its setup role.
     *
     * <p>Use this entry point for the <em>given</em> part of a scenario, where the test prepares workflow state before
     * the main behavior is triggered. The returned object is the shared action phase instance used by this fixture.
     * Failures raised while driving the fixture from this phase are reported as
     * {@link WorkflowTestFixtureException}.</p>
     *
     * @return action phase for arranging workflow state
     */
    ACTION given();

    /**
     * Returns the action phase in its stimulus role.
     *
     * <p>This method returns the same phase instance as {@link #given()}. The different name exists so tests can read
     * naturally when they move from setup to the behavior under test. Calling {@code when()} does not reset the fixture
     * or change the selected workflow state. Failures raised while driving the fixture from this phase are reported as
     * {@link WorkflowTestFixtureException}.</p>
     *
     * @return action phase for triggering the behavior under test
     */
    ACTION when();

    /**
     * Returns the assertion phase.
     *
     * <p>The returned phase shares the same workflow runtime and testing state as {@link #given()} and
     * {@link #when()}. Use it to assert executions, histories, steps, payload, and workflow state after one or more
     * action-phase operations. Failures raised from this phase are reported as
     * {@link AssertionError AssertionErrors}.</p>
     *
     * @return assertion phase for verifying workflow behavior
     */
    ASSERT then();
}
