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

import org.axonframework.common.annotation.Internal;

/**
 * Interface for handling failures in assertions.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public interface AssertionFailureHandler {

    /**
     * Constructs a new failure handler.
     *
     * @param throwFixtureException whether to fail assertions.  If set to {@code false}, the failures are delivered as
     *                      {@link AssertionError}s, else if set to {@code true} as
     *                      {@link WorkflowTestFixtureException}s.
     * @return failure handler.
     */
    static AssertionFailureHandler handler(boolean throwFixtureException) {
        return throwFixtureException ? new ExceptionFailureHandler() : new AssertionErrorFailureHandler();
    }

    /**
     * Fail the test with the given message.
     *
     * @param message the message to fail with.
     * @param cause   the cause of the failure.
     */
    void fail(String message, Throwable cause);

    /**
     * Failure handler that throws an {@link WorkflowTestFixtureException}.
     */
    @Internal
    class ExceptionFailureHandler implements AssertionFailureHandler {

        @Override
        public void fail(String message, Throwable cause) {
            throw new WorkflowTestFixtureException(message);
        }
    }

    /**
     * Failure handler that throws an {@link AssertionError}.
     */
    @Internal
    class AssertionErrorFailureHandler implements AssertionFailureHandler {

        @Override
        public void fail(String message, Throwable cause) {
            throw new AssertionError(message);
        }
    }
}
