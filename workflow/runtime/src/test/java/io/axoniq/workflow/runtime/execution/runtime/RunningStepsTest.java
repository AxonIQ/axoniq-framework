/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.execution.runtime;

import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import org.axonframework.common.infra.ComponentDescriptor;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test for {@link RunningSteps}.
 */
class RunningStepsTest {

    private RunningSteps runningSteps;

    @BeforeEach
    void setUp() {
        runningSteps = new RunningSteps();
    }

    @Test
    void testRegisterAndRemove() {
        String stepName = "testStep";
        CompletableFuture<Void> future = new CompletableFuture<>();

        runningSteps.register(stepName, future);
        runningSteps.remove(stepName);

        ComponentDescriptor descriptor = mock(ComponentDescriptor.class);
        runningSteps.describeTo(descriptor);
        verify(descriptor).describeProperty("runningSteps", List.of());
    }

    @Test
    void testCancelAndRemove() {
        String stepName = "testStep";
        CompletableFuture<Void> future = new CompletableFuture<>();

        runningSteps.register(stepName, future);
        runningSteps.cancelAndRemove(stepName, true);

        assertTrue(future.isCancelled());

        ComponentDescriptor descriptor = mock(ComponentDescriptor.class);
        runningSteps.describeTo(descriptor);
        verify(descriptor).describeProperty("runningSteps", List.of());
    }

    @Test
    void testCancelWithCause() {
        String stepName = "testStep";
        CompletableFuture<Void> future = new CompletableFuture<>();
        Exception cause = new RuntimeException("Test Cause");

        runningSteps.register(stepName, future);
        boolean result = runningSteps.cancelWithCause(stepName, cause);

        assertTrue(result);
        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertEquals(cause, ex.getCause());
    }

    @Test
    void testCancelWithCauseNull() {
        String stepName = "testStep";
        CompletableFuture<Void> future = new CompletableFuture<>();

        runningSteps.register(stepName, future);
        boolean result = runningSteps.cancelWithCause(stepName, null);

        assertTrue(result);
        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(StepCancellationException.class, ex.getCause());
    }

    @Test
    void testCancelWithCauseMissingStep() {
        boolean result = runningSteps.cancelWithCause("missing", null);
        assertFalse(result);
    }

    @Test
    void testCancelAll() {
        String step1 = "step1";
        String step2 = "step2";
        CompletableFuture<Void> future1 = new CompletableFuture<>();
        CompletableFuture<Void> future2 = new CompletableFuture<>();
        Exception cause = new RuntimeException("All stopped");

        runningSteps.register(step1, future1);
        runningSteps.register(step2, future2);

        AtomicReference<Set<String>> cancelledSteps = new AtomicReference<>();
        runningSteps.cancelAll(cause, cancelledSteps::set);

        assertEquals(Set.of(step1, step2), cancelledSteps.get());
        assertTrue(future1.isCompletedExceptionally());
        assertTrue(future2.isCompletedExceptionally());

        ExecutionException ex1 = assertThrows(ExecutionException.class, future1::get);
        assertEquals(cause, ex1.getCause());

        ComponentDescriptor descriptor = mock(ComponentDescriptor.class);
        runningSteps.describeTo(descriptor);
        verify(descriptor).describeProperty("runningSteps", List.of());
    }

    @Test
    void testDescribeTo() {
        String stepName = "testStep";
        CompletableFuture<Void> future = new CompletableFuture<>();
        runningSteps.register(stepName, future);

        ComponentDescriptor descriptor = mock(ComponentDescriptor.class);
        runningSteps.describeTo(descriptor);

        verify(descriptor).describeProperty("runningSteps", List.of(stepName));
    }
}
