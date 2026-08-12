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
package io.axoniq.workflow.runtime.execution;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link WorkflowEngineReplaySupport}.
 */
class WorkflowEngineReplaySupportTest {

    @Test
    void invokesTheInlineCallbackOnlyForTheFirstLiveModeTransition() {
        var callbackInvocations = new AtomicInteger();
        var support = new WorkflowEngineReplaySupport(context -> callbackInvocations.incrementAndGet());
        var context = mock(ProcessingContext.class);

        assertThat(support.switchToLiveMode(context)).isTrue();
        assertThat(support.switchToLiveMode(context)).isFalse();

        assertThat(support.inLiveMode()).isTrue();
        assertThat(callbackInvocations).hasValue(1);
    }

    @Test
    void activatesLiveModeWhenTheReplayPositionCoversTheStartupBoundary() {
        var callbackInvocations = new AtomicInteger();
        var support = new WorkflowEngineReplaySupport(context -> callbackInvocations.incrementAndGet());
        TrackingToken boundary = new GlobalSequenceTrackingToken(42);

        support.setInitialEngineTokens(new GlobalSequenceTrackingToken(18), boundary);
        support.validateIfReplayFinished(new GlobalSequenceTrackingToken(42), mock(ProcessingContext.class));

        assertThat(support.inLiveMode()).isTrue();
        assertThat(callbackInvocations).hasValue(1);
    }
}
