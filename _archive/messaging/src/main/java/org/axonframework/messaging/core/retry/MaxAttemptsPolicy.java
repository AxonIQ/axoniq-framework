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

package org.axonframework.messaging.core.retry;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;

import java.util.List;

/**
 * A retry policy that caps another policy to maximum number of retries
 */
public class MaxAttemptsPolicy implements RetryPolicy {

    private final RetryPolicy delegate;
    private final int maxAttempts;

    /**
     * Wraps the given {@code delegate}, enforcing the given maximum number of {@code retries}
     *
     * @param delegate The policy to use until the maximum number of retries is achieved
     * @param retries  The maximum number of retries to allow
     */
    public MaxAttemptsPolicy(RetryPolicy delegate, int retries) {
        this.delegate = delegate;
        this.maxAttempts = retries;
    }

    @Override
    public Outcome defineFor(Message message, Throwable cause,
                             List<Class<? extends Throwable>[]> previousFailures) {
        if (previousFailures.size() < maxAttempts) {
            return delegate.defineFor(message, cause, previousFailures);
        } else {
            return Outcome.doNotReschedule();
        }
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
        descriptor.describeProperty("maxAttempts", maxAttempts);
    }
}
