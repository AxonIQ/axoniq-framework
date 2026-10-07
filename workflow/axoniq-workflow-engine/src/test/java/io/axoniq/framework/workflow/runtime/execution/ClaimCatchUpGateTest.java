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
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the restored workflows of a claimed segment start once the segment reaches the head this node saw at
 * start-up, not the head at the time of the claim.
 */
class ClaimCatchUpGateTest {

    @Test
    void longRunningNodeStartsRestoredBodiesForASegmentThatIsStillBehindTheCurrentHead() {
        WorkflowEngineCatchUpSupport gate = new WorkflowEngineCatchUpSupport();
        // the node started when the head was at 5
        gate.initialize(new GlobalSequenceTrackingToken(5));
        // much later the head is at 100, and the node takes over a segment whose stored token is at 50
        boolean startsNow = gate.startsAfterClaim(new Segment(3, 15), new GlobalSequenceTrackingToken(50));
        // gap present: the bodies start although events 51..100 of this segment are still to be replayed
        assertThat(startsNow).isTrue();
    }

    @Test
    void freshNodeWaitsForCatchUp() {
        WorkflowEngineCatchUpSupport gate = new WorkflowEngineCatchUpSupport();
        gate.initialize(new GlobalSequenceTrackingToken(100));
        assertThat(gate.startsAfterClaim(new Segment(3, 15), new GlobalSequenceTrackingToken(50))).isFalse();
    }
}
