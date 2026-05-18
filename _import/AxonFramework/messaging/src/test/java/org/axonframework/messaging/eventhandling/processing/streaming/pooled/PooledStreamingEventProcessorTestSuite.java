/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.messaging.eventhandling.processing.streaming.pooled;

import org.awaitility.Awaitility;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.junit.jupiter.api.*;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Abstract integration test suite for {@link PooledStreamingEventProcessor}.
 *
 * @since 5.1.1
 */
public abstract class PooledStreamingEventProcessorTestSuite {

    protected String processorName() {
        return "PooledStreamingIT";
    }

    protected abstract AxonConfiguration axonConfiguration();

    protected PooledStreamingEventProcessor processor() {
        AxonConfiguration config = axonConfiguration();
        Configuration moduleConfig = config
                .getModuleConfiguration("EventProcessor[" + processorName() + "]")
                .orElseThrow(() -> new IllegalStateException(
                        "Module 'EventProcessor[" + processorName() + "]' not found. "));
        return moduleConfig
                .getOptionalComponent(StreamingEventProcessor.class, processorName())
                .map(p -> (PooledStreamingEventProcessor) p)
                .orElseThrow(() -> new IllegalStateException(
                        "PooledStreamingEventProcessor '" + processorName() + "' not found in module config"));
    }

    @AfterEach
    void tearDown() {
        cleanTokenStore();
    }

    /**
     * Subclass resets the token store state for test isolation.
     */
    protected abstract void cleanTokenStore();

    @Nested
    class WhenSplittingSegments {

        @Test
        void splitSegment_returnsTrue_andBothNewSegmentsBecomeClaimed() throws Exception {
            // given
            Awaitility.await()
                      .atMost(10, TimeUnit.SECONDS)
                      .until(() -> processor().processingStatus().containsKey(0));

            // when
            boolean splitResult = processor().splitSegment(0).get(15, TimeUnit.SECONDS);
            assertTrue(splitResult,
                       "splitSegment(0) should return true — if it throws UnableToClaimTokenException, the bug is present");

            // then
            Awaitility.await()
                      .atMost(10, TimeUnit.SECONDS)
                      .until(() -> processor().processingStatus().size() == 2);
            assertTrue(processor().processingStatus().containsKey(0),
                       "Segment 0 (re-initialized with new mask) must be active after split");
            assertTrue(processor().processingStatus().containsKey(1),
                       "Segment 1 (sibling) must be active after split");
        }
    }
}
