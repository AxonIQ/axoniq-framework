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

package org.axonframework.test.util;


import org.apache.commons.lang3.tuple.Pair;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class RecordingMessageMonitorTest {

    @Test
    void callsOnReportHooksWhenIngesting() {
        var success = new AtomicReference<Message>();
        var failure = new AtomicReference<Pair<Message, Throwable>>();
        var ignore = new AtomicReference<Message>();

        var successMessage = new GenericEventMessage(new MessageType("event"), "Success");
        var failureMessage = new GenericEventMessage(new MessageType("event"), "Failure");
        var exception = new RuntimeException("Test");
        var ignoreMessage = new GenericEventMessage(new MessageType("event"), "Ignore");

        var monitor = new RecordingMessageMonitor() {
            @Override
            protected void onReportSuccess(@NonNull Message message) {
                success.set(message);
            }

            @Override
            protected void onReportFailure(@NonNull Message message, @NonNull Throwable cause) {
                failure.set(Pair.of(message, cause));
            }

            @Override
            protected void onReportIgnored(@NonNull Message message) {
                ignore.set(message);
            }
        };

        monitor.onMessageIngested(successMessage).reportSuccess();
        monitor.onMessageIngested(failureMessage).reportFailure(exception);
        monitor.onMessageIngested(ignoreMessage).reportIgnored();

        assertThat(monitor.report().successReports()).hasSize(1);
        assertThat(monitor.report().failureReports()).hasSize(1);
        assertThat(monitor.report().ignoredReports()).hasSize(1);

        assertThat(success).hasValue(successMessage);
        assertThat(ignore).hasValue(ignoreMessage);
        assertThat(failure).hasValue(Pair.of(failureMessage, exception));
    }
}
