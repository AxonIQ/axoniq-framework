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

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Builds the {@link WorkflowExecution}/{@link WorkflowExecutionOperations}/{@link WorkflowConfiguration} mock chain
 * {@code WorkflowEngine} tests need to have the engine materialize an instance, without each test hand-rolling the
 * plumbing between them.
 * <p>
 * {@link #mockExecution(String, WorkflowState, boolean)} wires the part every engine test needs identically: a
 * {@link WorkflowExecutionOperations} whose {@link ProcessingContext#whenComplete(Consumer)} invokes its callback
 * synchronously, exactly as a real body context does when it has nothing pending.
 * {@link #mockConfiguration(String, WorkflowExecution)} wires the factory chain that hands that execution back for the
 * given workflow id. What varies per test - registering the configuration on the registry, and stubbing
 * {@code execute(...)} to observe body starts - stays in the test, where the difference is the point.
 *
 * @author Stefan Dragisic
 * @author Steven van Beelen
 */
final class WorkflowExecutionFixture {

    private WorkflowExecutionFixture() {
        // Utility class
    }

    /**
     * Builds a {@link WorkflowExecution} mock for {@code workflowId}, with its {@link WorkflowExecutionOperations}
     * wired so that {@code workflowExecutionOperations().processingContext().whenComplete(...)} invokes the callback
     * immediately.
     * <p>
     * Callers add their own {@code execute(any())} stubbing, since what a test wants to observe about a body start -
     * counting it, capturing its termination handler, or nothing at all - is specific to that test.
     *
     * @param workflowId id the execution reports through {@link WorkflowExecution#workflowId()}
     * @param state      state the execution reports through {@link WorkflowExecution#state()}
     * @param running    whether the execution reports {@link WorkflowExecution#isRunning()} as {@code true}
     */
    static WorkflowExecution mockExecution(String workflowId, WorkflowState state, boolean running) {
        var execution = mock(CancellationCapableExecution.class);
        when(execution.workflowId()).thenReturn(workflowId);
        when(execution.state()).thenReturn(state);
        when(execution.isRunning()).thenReturn(running);
        when(execution.workflowCancellation()).thenReturn(mock(WorkflowCancellation.class));
        when(execution.execute(any())).thenReturn(CompletableFuture.completedFuture(null));

        var workflowExecutionOperations = mock(WorkflowExecutionOperations.class);
        var bodyContext = mock(ProcessingContext.class);
        when(workflowExecutionOperations.processingContext()).thenReturn(bodyContext);
        when(bodyContext.whenComplete(any())).thenAnswer(invocation -> {
            invocation.<Consumer<ProcessingContext>>getArgument(0).accept(bodyContext);
            return bodyContext;
        });
        when(execution.workflowExecutionOperations()).thenReturn(workflowExecutionOperations);
        return execution;
    }

    /**
     * Builds a {@link WorkflowConfiguration} mock whose {@link WorkflowContextFactory}/{@link WorkflowExecutionFactory}
     * chain hands {@code execution} back for {@code workflowId}.
     * <p>
     * Callers add their own {@code workflowIdProvider()}/{@code workflowVersion()} stubbing and register the
     * configuration on the registry the way their scenario needs it: as the highest-version start candidate, as the
     * exact definition a durable id resolves to, or on a real registry.
     *
     * @param workflowId id {@link WorkflowContextFactory#createContext} is expected to be invoked with
     * @param execution  the execution {@link WorkflowExecutionFactory#create} hands back for that id's context
     */
    @SuppressWarnings("unchecked")
    static WorkflowConfiguration<WorkflowContext> mockConfiguration(String workflowId, WorkflowExecution execution) {
        var workflowContext = mock(WorkflowContext.class);

        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory executionFactory = mock(WorkflowExecutionFactory.class);
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        when(contextFactory.createContext(anyMap(), eq(workflowId), any(), eq(configuration)))
                .thenReturn(workflowContext);
        when(executionFactory.create(workflowContext)).thenReturn(execution);
        return configuration;
    }

    /**
     * Stubs {@code execution.execute(any())} to record the workflow id on {@code bodyStarts}, the observation channel
     * most engine tests count body starts through.
     */
    static void recordBodyStartOn(WorkflowExecution execution, Consumer<String> bodyStarts, String workflowId) {
        doAnswer(invocation -> {
            bodyStarts.accept(workflowId);
            return CompletableFuture.completedFuture(null);
        }).when(execution).execute(any());
    }

    interface CancellationCapableExecution extends WorkflowExecution, WorkflowCancellationProvider {

    }
}
