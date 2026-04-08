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

package org.axonframework.eventsourcing.eventstore.jpa;

import org.axonframework.common.AxonConfigurationException;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link AggregateBasedJpaEventStorageEngineConfiguration}.
 *
 * @author Steven van Beelen
 */
class AggregateBasedJpaEventStorageEngineConfigurationTest {

    @Test
    void nullFinalBatchPredicateThrowsException() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> AggregateBasedJpaEventStorageEngineConfiguration.DEFAULT.finalBatchPredicate(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void zeroBatchSizeThrowsException() {
        assertThatThrownBy(() -> AggregateBasedJpaEventStorageEngineConfiguration.DEFAULT.batchSize(0))
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void negativeBatchSizeThrowsException() {
        assertThatThrownBy(() -> AggregateBasedJpaEventStorageEngineConfiguration.DEFAULT.batchSize(-1))
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void negativeGapCleaningThresholdThrowsException() {
        assertThatThrownBy(() -> AggregateBasedJpaEventStorageEngineConfiguration.DEFAULT.gapCleaningThreshold(-1))
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void negativeMaxGapOffsetThrowsException() {
        assertThatThrownBy(() -> AggregateBasedJpaEventStorageEngineConfiguration.DEFAULT.maxGapOffset(-1))
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void negativeLowestGlobalSequenceThrowsException() {
        assertThatThrownBy(() -> AggregateBasedJpaEventStorageEngineConfiguration.DEFAULT.lowestGlobalSequence(-1))
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void negativeGapTimeoutThrowsException() {
        assertThatThrownBy(() -> AggregateBasedJpaEventStorageEngineConfiguration.DEFAULT.gapTimeout(-1))
                .isInstanceOf(AxonConfigurationException.class);
    }
}