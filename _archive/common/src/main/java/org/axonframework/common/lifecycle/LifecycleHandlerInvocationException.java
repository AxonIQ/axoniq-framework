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

package org.axonframework.common.lifecycle;

import org.jspecify.annotations.Nullable;

/**
 * Exception indicating a failure occurred during a lifecycle handler method invocation.
 *
 * @author Steven van Beelen
 * @since 4.3.0
 */
public class LifecycleHandlerInvocationException extends RuntimeException {

    /**
     * Instantiates an exception using the given {@code message} and {@code cause} indicating a failure during a
     * lifecycle handler method invocation.
     *
     * @param message The message describing the exception.
     * @param cause   The underlying cause of the exception.
     */
    public LifecycleHandlerInvocationException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
