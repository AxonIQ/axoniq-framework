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

import java.util.concurrent.TimeUnit;

final class RetryOutcome implements RetryPolicy.Outcome {

    private final long interval;
    private final TimeUnit timeUnit;

    public RetryOutcome(long interval, TimeUnit timeUnit) {
        this.interval = interval;
        this.timeUnit = timeUnit;
    }

    @Override
    public boolean shouldReschedule() {
        return true;
    }

    @Override
    public long rescheduleInterval() {
        return interval;
    }

    @Override
    public TimeUnit rescheduleIntervalTimeUnit() {
        return timeUnit;
    }
}
