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

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepFailedException;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher;
import io.axoniq.framework.workflow.runtime.test.utils.ManualExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.test.utils.ManualWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.test.utils.TestClock;
import io.axoniq.framework.workflow.runtime.test.utils.TestEventPublisher;
import org.awaitility.Awaitility;
import org.axonframework.common.configuration.AxonConfiguration;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link GivenWhen}.
 *
 * @author Simon Zambrovski
 */
class GivenWhenTest {

    @BeforeEach
    void shortenAwaitilityTimeout() {
        Awaitility.setDefaultTimeout(Duration.ofMillis(50));
        Awaitility.setDefaultPollInterval(Duration.ofMillis(5));
        Awaitility.setDefaultPollDelay(Duration.ZERO);
    }

    @AfterEach
    void resetAwaitilityTimeout() {
        Awaitility.reset();
    }

    @Test
    void whenReturnsSelf() {
        var phase = new GivenWhen.Phase();

        assertThat(phase.when()).isSameAs(phase);
    }

    @Test
    void thenReturnsFixtureThenPhase() {
        var phase = new GivenWhen.Phase();
        var thenPhase = new Then.Phase();
        @SuppressWarnings("rawtypes")
        WorkflowTestFixture fixture = mock(WorkflowTestFixture.class);
        when(fixture.then()).thenReturn(thenPhase);
        phase.fixture = fixture;

        assertThat(phase.then()).isSameAs(thenPhase);
    }

    @Test
    void publishEventReturnsSelfAndPublishesEvent() {
        var eventPublisher = mock(TestEventPublisher.class);
        when(eventPublisher.publish("event")).thenReturn(CompletableFuture.completedFuture(null));
        var phase = phaseWithDriver(driverWith(eventPublisher, false));

        assertThat(phase.publishEvent("event")).isSameAs(phase);
        verify(eventPublisher).publish("event");
    }

    @Test
    void executeStepReturnsSelfAndUsesOriginalAction() {
        var resolver = mock(ManualExecuteStepActionResolver.class);
        var state = workflowState(List.of("approve"),
                                  Map.of(),
                                  step("approve", StepStatus.COMPLETED));
        var phase = phaseWithDriver(stepperDriverWith(state, resolver, mock(TestClock.class),
                                                      mock(ManualWorkflowScheduler.class)));

        assertThat(phase.execute("approve")).isSameAs(phase);
        verify(resolver).applyOriginalAction("approve");
    }

    @Test
    void executeStepWithPayloadProcessorReturnsSelfAndUsesProvidedAction() {
        var resolver = mock(ManualExecuteStepActionResolver.class);
        var state = workflowState(List.of("approve"),
                                  Map.of(),
                                  step("approve", StepStatus.COMPLETED));
        var phase = phaseWithDriver(stepperDriverWith(state, resolver, mock(TestClock.class),
                                                      mock(ManualWorkflowScheduler.class)));

        assertThat(phase.execute("approve", (payload, context) -> Map.of())).isSameAs(phase);
        verify(resolver).applyAction(eq("approve"), any());
    }

    @Test
    void executeReturningReturnsSelf() {
        var resolver = mock(ManualExecuteStepActionResolver.class);
        var state = workflowState(List.of("approve"),
                                  Map.of(),
                                  step("approve", StepStatus.COMPLETED));
        var phase = phaseWithDriver(stepperDriverWith(state, resolver, mock(TestClock.class),
                                                      mock(ManualWorkflowScheduler.class)));

        assertThat(phase.executeReturning("approve", Map.of("ok", true))).isSameAs(phase);
        verify(resolver).applyAction(eq("approve"), any());
    }

    @Test
    void executeFailingReturnsSelf() {
        var resolver = mock(ManualExecuteStepActionResolver.class);
        var state = workflowState(List.of("approve"),
                                  Map.of(),
                                  step("approve", StepStatus.FAILED));
        var phase = phaseWithDriver(stepperDriverWith(state, resolver, mock(TestClock.class),
                                                      mock(ManualWorkflowScheduler.class)));

        assertThat(phase.executeFailing("approve", new StepFailedException("boom"))).isSameAs(phase);
        verify(resolver).applyAction(eq("approve"), any());
    }

    @Test
    void timePassesReturnsSelfAndAdvancesClock() {
        var clock = mock(TestClock.class);
        var scheduler = mock(ManualWorkflowScheduler.class);
        var now = Instant.parse("2026-06-26T12:00:00Z");
        when(clock.advanceBy(Duration.ofSeconds(5))).thenReturn(now);
        var phase = phaseWithDriver(stepperDriverWith(workflowState(List.of(), Map.of()),
                                                      mock(ManualExecuteStepActionResolver.class),
                                                      clock,
                                                      scheduler));

        assertThat(phase.timePasses(Duration.ofSeconds(5))).isSameAs(phase);
        verify(clock).advanceBy(Duration.ofSeconds(5));
        verify(scheduler).runDueTasks(now);
    }

    private GivenWhen.Phase phaseWithDriver(WorkflowTestDriver driver) {
        var phase = new GivenWhen.Phase();
        phase.testDriver = driver;
        return phase;
    }

    private WorkflowTestDriver driverWith(TestEventPublisher eventPublisher, boolean steppingMode) {

        var configuration = mock(AxonConfiguration.class);
        when(configuration.getComponent(WorkflowEngine.class)).thenReturn(mock(WorkflowEngine.class));
        when(configuration.getComponent(DelayedPublisher.class)).thenReturn(mock(DelayedPublisher.class));
        when(configuration.getComponent(TestEventPublisher.class)).thenReturn(eventPublisher);
        when(configuration.getComponent(WorkflowConfigurationRegistry.class)).thenReturn(mock(WorkflowConfigurationRegistry.class));
        when(configuration.getComponent(MutableWorkflowHistoryRepository.class)).thenReturn(
                mock(MutableWorkflowHistoryRepository.class));

        when(configuration.getOptionalComponent(TestClock.class)).thenReturn(Optional.of(mock(TestClock.class)));
        when(configuration.getOptionalComponent(ManualExecuteStepActionResolver.class)).thenReturn(Optional.of(mock(
                ManualExecuteStepActionResolver.class)));
        when(configuration.getOptionalComponent(ManualWorkflowScheduler.class)).thenReturn(Optional.of(mock(
                ManualWorkflowScheduler.class)));

        var services = WorkflowTestServices.from(configuration);

        return new DefaultWorkflowTestDriver(services,
                                             new MutableWorkflowEngineTestingState(),
                                             AssertionFailureHandler.handler(true),
                                             steppingMode);
    }

    private WorkflowTestDriver stepperDriverWith(WorkflowState state,
                                                 ManualExecuteStepActionResolver resolver,
                                                 TestClock clock,
                                                 ManualWorkflowScheduler scheduler) {
        var historyRepository = mock(MutableWorkflowHistoryRepository.class);
        when(historyRepository.findAll()).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(
                List.of(new WorkflowHistory("wf-1", state))
        ));
        var configuration = mock(AxonConfiguration.class);
        when(configuration.getComponent(WorkflowEngine.class)).thenReturn(mock(WorkflowEngine.class));
        when(configuration.getComponent(DelayedPublisher.class)).thenReturn(mock(DelayedPublisher.class));
        when(configuration.getComponent(TestEventPublisher.class)).thenReturn(mock(TestEventPublisher.class));
        when(configuration.getComponent(WorkflowConfigurationRegistry.class)).thenReturn(mock(WorkflowConfigurationRegistry.class));
        when(configuration.getComponent(MutableWorkflowHistoryRepository.class)).thenReturn(historyRepository);

        when(configuration.getOptionalComponent(TestClock.class)).thenReturn(Optional.of(clock));
        when(configuration.getOptionalComponent(ManualExecuteStepActionResolver.class)).thenReturn(Optional.of(resolver));
        when(configuration.getOptionalComponent(ManualWorkflowScheduler.class)).thenReturn(Optional.of(scheduler));

        var services = WorkflowTestServices.from(configuration);


        var testingState = new MutableWorkflowEngineTestingState();
        testingState.setHistory(new WorkflowHistory("wf-1", state));
        return new DefaultWorkflowTestDriver(services,
                                             testingState,
                                             AssertionFailureHandler.handler(true),
                                             true);
    }

    private WorkflowState workflowState(List<String> stepNames,
                                        Map<String, @Nullable Object> payload,
                                        WorkflowStep... steps) {
        WorkflowState state = mock(WorkflowState.class);
        when(state.workflowStepNames()).thenReturn(stepNames);
        when(state.payload()).thenReturn(payload);
        for (WorkflowStep step : steps) {
            when(state.containsStep(step.stepName())).thenReturn(true);
            when(state.getStep(step.stepName())).thenReturn(step);
        }
        return state;
    }

    private WorkflowStep step(String stepName, StepStatus stepStatus) {
        return switch (stepStatus) {
            case STARTED -> WorkflowStep.started(stepName, Map.of(), Instant.EPOCH, null);
            case COMPLETED -> WorkflowStep.completed(stepName, Map.of(), Instant.EPOCH, null);
            case FAILED -> WorkflowStep.failed(stepName, new RuntimeException("boom"), Instant.EPOCH, null);
            case TIMED_OUT -> WorkflowStep.timedOut(stepName, Map.of(), Instant.EPOCH, null);
            case CANCELLED -> WorkflowStep.cancelled(stepName, Instant.EPOCH, null);
            case RETRYING -> throw new IllegalArgumentException("Retrying is not used in these tests");
        };
    }
}
