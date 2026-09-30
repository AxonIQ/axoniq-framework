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

import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher;
import io.axoniq.framework.workflow.runtime.test.utils.ManualExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.test.utils.ManualWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.test.utils.TestClock;
import io.axoniq.framework.workflow.runtime.test.utils.TestEventPublisher;
import org.axonframework.common.configuration.AxonConfiguration;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.Optional;

import static io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestServices.DEFAULT_MODULE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link WorkflowTestServices}.
 *
 * @author Simon Zambrovski
 */
class WorkflowTestServicesTest {

    @Test
    void fromCollectsConfiguredServicesAndLazySteppingServices() {
        AxonConfiguration configuration = mock(AxonConfiguration.class);
        WorkflowEngine workflowEngine = mock(WorkflowEngine.class);
        DelayedPublisher delayedPublisher = mock(DelayedPublisher.class);
        TestEventPublisher eventPublisher = mock(TestEventPublisher.class);
        WorkflowConfigurationRegistry<?> workflowRegistry = mock(WorkflowConfigurationRegistry.class);
        MutableWorkflowHistoryRepository workflowHistoryRepository = mock(MutableWorkflowHistoryRepository.class);
        TestClock clock = mock(TestClock.class);
        ManualExecuteStepActionResolver actionResolver = mock(ManualExecuteStepActionResolver.class);
        ManualWorkflowScheduler timeoutScheduler = mock(ManualWorkflowScheduler.class);
        when(configuration.getComponents(WorkflowEngine.class))
                .thenReturn(Map.of("WorkflowEngine[" + DEFAULT_MODULE_NAME + "]", workflowEngine));
        when(configuration.getComponent(DelayedPublisher.class)).thenReturn(delayedPublisher);
        when(configuration.getComponent(TestEventPublisher.class)).thenReturn(eventPublisher);
        when(configuration.getComponent(WorkflowConfigurationRegistry.class)).thenReturn(workflowRegistry);
        when(configuration.getComponent(MutableWorkflowHistoryRepository.class)).thenReturn(workflowHistoryRepository);
        when(configuration.getOptionalComponent(TestClock.class)).thenReturn(Optional.of(clock));
        when(configuration.getOptionalComponent(ManualExecuteStepActionResolver.class)).thenReturn(
                Optional.of(actionResolver));
        when(configuration.getOptionalComponent(ManualWorkflowScheduler.class))
                .thenReturn(Optional.of(timeoutScheduler));

        WorkflowTestServices services = WorkflowTestServices.from(configuration);

        assertThat(services.configuration()).isSameAs(configuration);
        assertThat(services.workflowEngine()).isSameAs(workflowEngine);
        assertThat(services.delayedPublisher()).isSameAs(delayedPublisher);
        assertThat(services.eventPublisher()).isSameAs(eventPublisher);
        assertThat(services.workflowRegistry()).isSameAs(workflowRegistry);
        assertThat(services.workflowHistoryRepository()).isSameAs(workflowHistoryRepository);
        assertThat(services.clock().get()).isSameAs(clock);
        assertThat(services.executeStepActionResolver().get()).isSameAs(actionResolver);
        assertThat(services.timeoutScheduler().get()).isSameAs(timeoutScheduler);
    }

    @Test
    void steppingServiceOptionalIsEmptyWhenComponentIsMissing() {
        AxonConfiguration configuration = baseConfiguration();
        when(configuration.getOptionalComponent(TestClock.class)).thenReturn(Optional.empty());

        WorkflowTestServices services = WorkflowTestServices.from(configuration);

        assertThat(services.clock()).isEmpty();
    }

    @Test
    void shutdownStopsEngineConfigurationAndClearsHistory() {
        AxonConfiguration configuration = baseConfiguration();
        WorkflowEngine workflowEngine = configuration.getComponents(WorkflowEngine.class).values().iterator().next();
        MutableWorkflowHistoryRepository historyRepository = configuration.getComponent(
                MutableWorkflowHistoryRepository.class);
        WorkflowTestServices services = WorkflowTestServices.from(configuration);

        services.shutdown();

        verify(workflowEngine).shutdown();
        verify(configuration).shutdown();
        verify(historyRepository).clear();
    }

    private static AxonConfiguration baseConfiguration() {
        AxonConfiguration configuration = mock(AxonConfiguration.class);
        when(configuration.getComponents(WorkflowEngine.class)).thenReturn(
                Map.of("WorkflowEngine[" + DEFAULT_MODULE_NAME + "]", mock(WorkflowEngine.class)));
        when(configuration.getComponent(DelayedPublisher.class)).thenReturn(mock(DelayedPublisher.class));
        when(configuration.getComponent(TestEventPublisher.class)).thenReturn(mock(TestEventPublisher.class));
        when(configuration.getComponent(WorkflowConfigurationRegistry.class)).thenReturn(
                mock(WorkflowConfigurationRegistry.class));
        when(configuration.getComponent(MutableWorkflowHistoryRepository.class)).thenReturn(
                mock(MutableWorkflowHistoryRepository.class));
        when(configuration.getOptionalComponent(TestClock.class)).thenReturn(
                Optional.of(mock(TestClock.class)));
        when(configuration.getOptionalComponent(ManualExecuteStepActionResolver.class)).thenReturn(
                Optional.of(mock(ManualExecuteStepActionResolver.class)));
        when(configuration.getOptionalComponent(ManualWorkflowScheduler.class)).thenReturn(
                Optional.of(mock(ManualWorkflowScheduler.class)));
        return configuration;
    }
}
