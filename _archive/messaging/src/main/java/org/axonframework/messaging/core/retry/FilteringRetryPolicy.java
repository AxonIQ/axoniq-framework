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
import java.util.function.Predicate;

/**
 * A RetryPolicy that delegates to another RetryPolicy when the latest exception matches a given predicate.
 */
public class FilteringRetryPolicy implements RetryPolicy {

    private final RetryPolicy delegate;
    private final Predicate<Throwable> retryableErrorPredicate;

    /**
     * Initializes a RetryPolicy to wrap given {@code delegate} RetryPolicy when the latest failure matches the given
     * {@code retryableErrorPredicate}
     *
     * @param delegate                The policy to delegate to
     * @param retryableErrorPredicate The predicate matching errors that can be retried
     */
    public FilteringRetryPolicy(RetryPolicy delegate, Predicate<Throwable> retryableErrorPredicate) {
        this.delegate = delegate;
        this.retryableErrorPredicate = retryableErrorPredicate;
    }

    @Override
    public Outcome defineFor(Message message, Throwable cause,
                             List<Class<? extends Throwable>[]> previousFailures) {
        if (retryableErrorPredicate.test(cause)) {
            return delegate.defineFor(message, cause, previousFailures);
        }
        return Outcome.doNotReschedule();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
        descriptor.describeProperty("filter", retryableErrorPredicate.toString());
    }
}
