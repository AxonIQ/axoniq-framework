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

package io.axoniq.framework.messaging.transformation;

import org.jspecify.annotations.Nullable;

/**
 * Thrown by {@code EventTransformerChain.Builder} on chain misconfiguration. The
 * exception message identifies the specific cause. Exceptions thrown from inside a
 * transformer's mapper propagate to the caller directly; this type covers
 * configuration errors detected by the framework.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
public final class ChainConfigurationException extends RuntimeException {

    /**
     * @param message human-readable description of the problem
     */
    public ChainConfigurationException(String message) {
        super(message);
    }

    /**
     * @param message human-readable description of the problem
     * @param cause   the underlying cause, may be {@code null}
     */
    public ChainConfigurationException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
