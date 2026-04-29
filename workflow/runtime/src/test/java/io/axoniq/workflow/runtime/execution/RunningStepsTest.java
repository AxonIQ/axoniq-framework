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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import org.axonframework.common.infra.ComponentDescriptor;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
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

        assertThat(future.isCancelled()).isTrue();

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

        assertThat(result).isTrue();
        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCause(cause);
    }

    @Test
    void testCancelWithCauseNull() {
        String stepName = "testStep";
        CompletableFuture<Void> future = new CompletableFuture<>();

        runningSteps.register(stepName, future);
        boolean result = runningSteps.cancelWithCause(stepName, null);

        assertThat(result).isTrue();
        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .extracting(Throwable::getCause)
                .isInstanceOf(StepCancellationException.class);
    }

    @Test
    void testCancelWithCauseMissingStep() {
        boolean result = runningSteps.cancelWithCause("missing", null);
        assertThat(result).isFalse();
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

        assertThat(cancelledSteps.get()).containsExactlyInAnyOrder(step1, step2);
        assertThat(future1.isCompletedExceptionally()).isTrue();
        assertThat(future2.isCompletedExceptionally()).isTrue();

        assertThatThrownBy(future1::get)
                .isInstanceOf(ExecutionException.class)
                .hasCause(cause);

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
