/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.workflow.runtime.test.configuration;

import io.axoniq.framework.workflow.runtime.execution.ExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.execution.WorkflowScheduler;
import io.axoniq.framework.workflow.runtime.test.utils.ManualExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.test.utils.ManualWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.test.utils.TestClock;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;

import java.time.Clock;

/**
 * Test configuration enhancer that switches workflow execution into stepping mode.
 *
 * <p>Stepping mode is intended for fixture-style workflow tests where the test should control when an
 * {@code execute(...)} step finishes and when scheduled workflow work becomes due. Instead of letting the runtime run
 * execute-step actions and time-based tasks immediately, this enhancer installs test doubles that pause those
 * transitions until the test explicitly releases them.</p>
 *
 * <p>This enhancer registers three key components:</p>
 * <ul>
 *     <li>a {@link ManualExecuteStepActionResolver}, exposed as the runtime {@link ExecuteStepActionResolver}, so
 *     execute steps block until the test supplies either the original action or a test action for the step name</li>
 *     <li>a mutable {@link TestClock}, exposed as the runtime {@link Clock}, so workflow time can be advanced
 *     deterministically by the test</li>
 *     <li>a {@link ManualWorkflowScheduler}, exposed as the runtime {@link WorkflowScheduler}, so scheduled work runs
 *     only when the test advances time and triggers due tasks</li>
 * </ul>
 *
 * <p>Use this enhancer when building custom test configurations that need the same stepping behavior as
 * {@link io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestDriver#stepper(io.axoniq.framework.workflow.configuration.WorkflowModule,
 * boolean, java.util.function.UnaryOperator)} or {@link io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestFixture}.
 * Those higher-level APIs already register this enhancer automatically. Register it directly only when you are wiring
 * the workflow test configuration yourself.</p>
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class WorkflowTestSteppingModeEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerComponent(ManualExecuteStepActionResolver.class, c -> new ManualExecuteStepActionResolver())
                .registerComponent(ExecuteStepActionResolver.class,
                                   c -> c.getComponent(ManualExecuteStepActionResolver.class))
                .registerComponent(TestClock.class, c -> new TestClock())
                .registerComponent(Clock.class, c -> c.getComponent(TestClock.class))
                .registerComponent(ManualWorkflowScheduler.class, c -> new ManualWorkflowScheduler())
                .registerComponent(WorkflowScheduler.class,
                                   c -> c.getComponent(ManualWorkflowScheduler.class))
        ;
    }
}
