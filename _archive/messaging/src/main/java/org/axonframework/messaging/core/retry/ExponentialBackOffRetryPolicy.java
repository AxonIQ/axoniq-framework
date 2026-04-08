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
import java.util.concurrent.TimeUnit;

/**
 * A RetryScheduler that uses a backoff strategy, doubling the retry delay after each attempt.
 *
 * @author Bert Laverman
 * @author Allard Buijze
 * @since 4.2
 */
public class ExponentialBackOffRetryPolicy implements RetryPolicy {

    private final long initialWaitTime;

    /**
     * Initializes an exponential delay policy with given {@code initialWaitTime} in milliseconds.
     *
     * @param initialWaitTime the wait time for the first retry
     */
    public ExponentialBackOffRetryPolicy(long initialWaitTime) {
        this.initialWaitTime = initialWaitTime;
    }

    @Override
    public Outcome defineFor(Message message, Throwable failure,
                             List<Class<? extends Throwable>[]> previousFailures) {
        if (Long.numberOfLeadingZeros(initialWaitTime) <= previousFailures.size()) {
            return Outcome.rescheduleIn(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
        long waitTime = initialWaitTime << previousFailures.size();
        return Outcome.rescheduleIn(waitTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("initialWaitTime", initialWaitTime);
    }
}
