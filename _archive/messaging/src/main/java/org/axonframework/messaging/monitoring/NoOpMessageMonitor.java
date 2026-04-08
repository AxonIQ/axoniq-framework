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

package org.axonframework.messaging.monitoring;

import org.axonframework.messaging.core.Message;

/**
 * A message monitor that returns a NoOp message callback
 *
 * @author Marijn van Zelst
 * @since 3.0
 */
public enum NoOpMessageMonitor implements MessageMonitor<Message> {

    /**
     * Singleton instance of a {@link NoOpMessageMonitor}.
     */
    INSTANCE;

    /**
     * Returns the instance of {@code {@link NoOpMessageMonitor}}.
     * This method is a convenience method, which can be used as a lambda expression
     *
     * @return the instance of {@code {@link NoOpMessageMonitor}}
     */
    @SuppressWarnings("SameReturnValue")
    public static NoOpMessageMonitor instance() {
        return INSTANCE;
    }

    @Override
    public MonitorCallback onMessageIngested(Message message) {
        return NoOpMessageMonitorCallback.INSTANCE;
    }

}
