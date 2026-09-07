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
package io.axoniq.framework.workflow.simulation;

import io.axoniq.framework.workflow.configuration.WorkflowConfigurationDefaults;
import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.runtime.execution.DefaultWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.execution.WorkflowScheduler;
import io.axoniq.framework.workflow.runtime.test.fakes.ManualWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.test.fakes.MutableClock;
import io.axoniq.framework.workflow.runtime.test.fakes.SameThreadExecutorService;
import org.axonframework.common.configuration.AxonConfiguration;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the determinism seams are wired: each seam resolves to its real implementation by default, and a fake can be
 * injected via the configurer (the surface the simulator uses).
 */
class DeterminismSeamWiringTest {

    // ---- WorkflowScheduler seam ----

    @Test
    void workflowScheduler_defaultsToTheWallClockImplementation() {
        try (var configuration = startConfiguration(WorkflowConfigurer.create())) {
            assertThat(configuration.get().getComponent(WorkflowScheduler.class))
                    .isInstanceOf(DefaultWorkflowScheduler.class);
        }
    }

    @Test
    void workflowScheduler_canBeReplacedWithFakeViaConfigurer() {
        var fake = new ManualWorkflowScheduler();
        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> cr.registerComponent(WorkflowScheduler.class, cfg -> fake));

        try (var configuration = startConfiguration(configurer)) {
            assertThat(configuration.get().getComponent(WorkflowScheduler.class)).isSameAs(fake);
        }
    }

    // ---- Clock seam ----

    @Test
    void clock_canBeReplacedWithMutableClockViaConfigurer() {
        var fake = new MutableClock();
        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> cr.registerComponent(Clock.class, cfg -> fake));

        try (var configuration = startConfiguration(configurer)) {
            assertThat(configuration.get().getComponent(Clock.class)).isSameAs(fake);
        }
    }

    // ---- Workflow body executor seam ----

    @Test
    void bodyExecutor_canBeReplacedWithSameThreadExecutorViaConfigurer() {
        var fake = new SameThreadExecutorService();
        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> cr.registerComponent(ExecutorService.class,
                                                                WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR,
                                                                cfg -> fake));

        try (var configuration = startConfiguration(configurer)) {
            assertThat(configuration.get().getComponent(ExecutorService.class,
                                                        WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR))
                    .isSameAs(fake);
        }
    }

    private static StartedConfiguration startConfiguration(WorkflowConfigurer configurer) {
        return new StartedConfiguration(configurer.start());
    }

    /**
     * Auto-closeable wrapper so each started configuration shuts down its engine after the assertion.
     */
    private record StartedConfiguration(AxonConfiguration configuration) implements AutoCloseable {

        AxonConfiguration get() {
            return configuration;
        }

        @Override
        public void close() {
            configuration.shutdown();
        }
    }
}
