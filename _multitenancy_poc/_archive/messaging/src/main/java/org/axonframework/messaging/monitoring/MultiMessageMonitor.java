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

import org.axonframework.common.Assert;
import org.axonframework.messaging.core.Message;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Delegates messages and callbacks to the given list of message monitors
 *
 * @param <T> the message type
 * @author Marijn van Zelst
 * @since 3.0
 */
public class MultiMessageMonitor<T extends Message> implements MessageMonitor<T> {

    private final List<MessageMonitor<? super T>> messageMonitors;

    /**
     * Initialize a message monitor with the given <name>messageMonitors</name>
     *
     * @param messageMonitors the list of event monitors to delegate to
     */
    @SafeVarargs
    public MultiMessageMonitor(MessageMonitor<? super T>... messageMonitors) {
        this(Arrays.asList(messageMonitors));
    }

    /**
     * Initialize a message monitor with the given list of <name>messageMonitors</name>
     *
     * @param messageMonitors the list of event monitors to delegate to
     */
    public MultiMessageMonitor(List<MessageMonitor<? super T>> messageMonitors) {
        Assert.notNull(messageMonitors, () -> "MessageMonitor list may not be null");
        this.messageMonitors = Collections.unmodifiableList(messageMonitors);
    }

    /**
     * Calls the message monitors with the given message and returns a callback that will trigger all the message
     * monitor callbacks
     *
     * @param message the message to delegate to the message monitors
     * @return the callback that will trigger all the message monitor callbacks
     */
    @Override
    public MonitorCallback onMessageIngested(T message) {
        final List<MonitorCallback> monitorCallbacks = messageMonitors.stream()
                                                                      .map(messageMonitor -> messageMonitor.onMessageIngested(
                                                                              message))
                                                                      .toList();

        return new MonitorCallback() {
            @Override
            public void reportSuccess() {
                monitorCallbacks.forEach(MonitorCallback::reportSuccess);
            }

            @Override
            public void reportFailure(Throwable cause) {
                monitorCallbacks.forEach(resultCallback -> resultCallback.reportFailure(cause));
            }

            @Override
            public void reportIgnored() {
                monitorCallbacks.forEach(MonitorCallback::reportIgnored);
            }
        };
    }

    /**
     * Inspect the contained message monitors.
     *
     * @return unmodifiable list of contained message monitors
     */
    public List<MessageMonitor<? super T>> messageMonitors() {
        return messageMonitors;
    }
}
