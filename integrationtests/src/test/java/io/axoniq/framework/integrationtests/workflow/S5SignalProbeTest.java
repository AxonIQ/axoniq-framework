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
package io.axoniq.framework.integrationtests.workflow;

import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Harness probe for the S5 rig: a single in-memory node must wake from the GoSignal park when the signal is appended
 * straight to the store (the way the S5 orchestrator injects it). If this fails, the S5 inconclusiveness is a signal
 * delivery problem in the rig, not a multi-JVM fencing property.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class S5SignalProbeTest {

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void appendedSignalWakesParkedInstance() throws Exception {
        var store = new InMemoryEventStorageEngine();
        String wfId = "probe-1";
        try (var node = DcbFencingBackends.startNode(store, "s5-probe", new S5NodeMain.SlowWorkflow("P", 0))) {
            node.publish(new S5NodeMain.StartSlow(wfId));
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                    assertThat(DcbFencingBackends.eventsNamed(store, wfId, "prepareCompleted")
                                       .size() + DcbFencingBackends.eventsWithTags(
                            store, org.axonframework.messaging.eventstreaming.Tag.of("workflowId", wfId)).size())
                            .isGreaterThan(1));

            node.publish(new S5NodeMain.GoSignal(wfId));

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(DcbFencingBackends.eventsNamed(store, wfId, "S5SlowWorkflowCompleted")).hasSize(1));
        }
    }
}
