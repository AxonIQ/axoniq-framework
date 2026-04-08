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

package io.axoniq.framework.axonserver.connector.util;

import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.ProcessingKey;
import org.junit.jupiter.api.*;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link ProcessingInstructionUtils}.
 *
 * @author Steven van Beelen
 */
class ProcessingInstructionUtilsTest {

    private static final long EXPECTED_VALUE = 1729L;
    private static final MetaDataValue TEST_META_DATA_VALUE = MetaDataValue.newBuilder()
                                                                           .setNumberValue(EXPECTED_VALUE)
                                                                           .build();

    @Test
    void priorityDefaultsToZero() {
        assertEquals(0L, ProcessingInstructionUtils.priority(Collections.emptyList()));
    }

    @Test
    void priority() {
        ProcessingInstruction testProcessingInstruction =
                ProcessingInstruction.newBuilder()
                                     .setKey(ProcessingKey.PRIORITY)
                                     .setValue(TEST_META_DATA_VALUE)
                                     .build();
        assertEquals(EXPECTED_VALUE,
                     ProcessingInstructionUtils.priority(Collections.singletonList(testProcessingInstruction)));
    }

    @Test
    void numberOfResultsDefaultsToZero() {
        assertEquals(1L, ProcessingInstructionUtils.numberOfResults(Collections.emptyList()));
    }

    @Test
    void numberOfResults() {
        ProcessingInstruction testProcessingInstruction =
                ProcessingInstruction.newBuilder()
                                     .setKey(ProcessingKey.NR_OF_RESULTS)
                                     .setValue(TEST_META_DATA_VALUE)
                                     .build();
        assertEquals(EXPECTED_VALUE,
                     ProcessingInstructionUtils.numberOfResults(Collections.singletonList(testProcessingInstruction)));
    }

    @Test
    void timeoutDefaultsToZero() {
        assertEquals(0L, ProcessingInstructionUtils.timeout(Collections.emptyList()));
    }

    @Test
    void timeoutDefaults() {
        ProcessingInstruction testProcessingInstruction =
                ProcessingInstruction.newBuilder()
                                     .setKey(ProcessingKey.TIMEOUT)
                                     .setValue(TEST_META_DATA_VALUE)
                                     .build();
        assertEquals(EXPECTED_VALUE,
                     ProcessingInstructionUtils.timeout(Collections.singletonList(testProcessingInstruction)));
    }
}