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
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.StepInterruptedException;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RecoverableWorkflowExceptionPolicy}.
 *
 * @author Stefan Dragisic
 */
class RecoverableWorkflowExceptionPolicyTest {

    private static final Predicate<Throwable> DEFAULT = RecoverableWorkflowExceptionPolicy.DEFAULT::isRecoverable;

    @Test
    void infrastructureAndJvmFaultsAreRecoverable() {
        assertThat(DEFAULT)
                .accepts(new OutOfMemoryError(),
                         new StackOverflowError(),
                         new InterruptedException(),
                         new StepInterruptedException("engine shutdown"),
                         new RejectedExecutionException(),
                         new TimeoutException(),
                         new IOException("connection reset"));
    }

    @Test
    void wrappedCausesAreInspected() {
        assertThat(DEFAULT)
                .accepts(new RuntimeException(new IOException("db down")),
                         new CompletionException(new TimeoutException()));
    }

    @Test
    void orWidensThePolicy() {
        // given
        var policy = RecoverableWorkflowExceptionPolicy.DEFAULT.or(e -> e instanceof IllegalStateException);

        // when / then
        assertThat(policy.isRecoverable(new IllegalStateException("backend unavailable"))).isTrue();
        assertThat(policy.isRecoverable(new IOException("db down"))).isTrue();
        assertThat(policy.isRecoverable(new NullPointerException())).isFalse();
    }

    @Test
    void codeDefectsAreNotRecoverable() {
        assertThat(DEFAULT)
                .rejects(new NullPointerException(),
                         new ClassCastException(),
                         new IllegalArgumentException("bad input"),
                         new IllegalStateException("bad state"),
                         new ArithmeticException(),
                         new RuntimeException("no cause"));
    }
}
