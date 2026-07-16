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
package io.axoniq.workflow.runtime.test.fixture;

import jakarta.annotation.Nonnull;

/**
 * Runtime exception used by the workflow test fixture to report assertion failures.
 *
 * <p>This exception is emitted when fixture-based assertions are configured to fail with exceptions instead of plain
 * {@link AssertionError AssertionErrors}. That mode is selected through {@link AssertionFailureHandler#handler(boolean)} and is used
 * by APIs such as {@link WorkflowTestDriver#stepper(io.axoniq.workflow.configuration.WorkflowModule, boolean,
 * java.util.function.UnaryOperator)} when the {@code throwFixtureException} flag is enabled.</p>
 *
 * <p>The exception typically carries the user-facing failure message produced by the fixture, for example when an
 * expected workflow execution, history entry, or terminal step state does not appear within the configured wait time.
 * It allows tests or higher-level fixture abstractions to distinguish workflow-fixture failures from ordinary
 * assertion failures or infrastructure exceptions.</p>
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class WorkflowTestFixtureException extends RuntimeException {

    /**
     * Constructs a new exception.
     *
     * @param message message of the exception
     */
    public WorkflowTestFixtureException(@Nonnull String message) {
        super(message);
    }

    /**
     * Constructs a new exception.
     *
     * @param message message of the exception
     * @param cause   cause of the exception
     */
    public WorkflowTestFixtureException(@Nonnull String message, @Nonnull Throwable cause) {
        super(message, cause);
    }
}
