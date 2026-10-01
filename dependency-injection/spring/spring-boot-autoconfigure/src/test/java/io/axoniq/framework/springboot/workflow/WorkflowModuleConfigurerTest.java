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
package io.axoniq.framework.springboot.workflow;

import io.axoniq.framework.springboot.WorkflowProperties;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link WorkflowModuleConfigurer}.
 *
 * @author Steven van Beelen
 */
class WorkflowModuleConfigurerTest {

    private static PooledStreamingEventProcessorConfiguration processorConfiguration() {
        return new PooledStreamingEventProcessorConfiguration(new EventProcessorConfiguration("Workflow", null));
    }

    @Nested
    class WhenNoPropertyIsSet {

        @Test
        void appliesTheSameDefaultsAsEventProcessorPropertiesProcessorSettings() {
            // given
            var properties = new WorkflowProperties();

            // when
            var result = WorkflowModuleConfigurer.applyProperties(properties, processorConfiguration());

            // then
            assertThat(result.initialSegmentCount()).isEqualTo(16);
            assertThat(result.batchSize()).isEqualTo(1);
            assertThat(result.maxSegmentProvider().getMaxSegments("Workflow")).isEqualTo(4);
            assertThat(result.tokenClaimInterval()).isEqualTo(5000L);
            assertThat(result.claimExtensionThreshold()).isEqualTo(5000L);
            assertThat(result.coordinatorExtendsClaims()).isFalse();
        }
    }

    @Nested
    class WhenPropertiesAreSet {

        @Test
        void appliesTheInitialSegmentCount() {
            var properties = new WorkflowProperties();
            properties.setInitialSegmentCount(4);

            var result = WorkflowModuleConfigurer.applyProperties(properties, processorConfiguration());

            assertThat(result.initialSegmentCount()).isEqualTo(4);
        }

        @Test
        void appliesTheBatchSize() {
            var properties = new WorkflowProperties();
            properties.setBatchSize(50);

            var result = WorkflowModuleConfigurer.applyProperties(properties, processorConfiguration());

            assertThat(result.batchSize()).isEqualTo(50);
        }

        @Test
        void appliesTheThreadCountAsTheMaxClaimedSegments() {
            var properties = new WorkflowProperties();
            properties.setThreadCount(2);

            var result = WorkflowModuleConfigurer.applyProperties(properties, processorConfiguration());

            assertThat(result.maxSegmentProvider().getMaxSegments("Workflow")).isEqualTo(2);
        }

        @Test
        void appliesTheTokenClaimInterval() {
            var properties = new WorkflowProperties();
            properties.setTokenClaimInterval(2500L);

            var result = WorkflowModuleConfigurer.applyProperties(properties, processorConfiguration());

            assertThat(result.tokenClaimInterval()).isEqualTo(2500L);
        }

        @Test
        void appliesTheClaimExtensionThreshold() {
            var properties = new WorkflowProperties();
            properties.setClaimExtensionThreshold(7500L);

            var result = WorkflowModuleConfigurer.applyProperties(properties, processorConfiguration());

            assertThat(result.claimExtensionThreshold()).isEqualTo(7500L);
        }

        @Test
        void enablesCoordinatorClaimExtensionWhenTrue() {
            var properties = new WorkflowProperties();
            properties.setCoordinatorClaimExtension(true);

            var result = WorkflowModuleConfigurer.applyProperties(properties, processorConfiguration());

            assertThat(result.coordinatorExtendsClaims()).isTrue();
        }

        @Test
        void doesNotEnableCoordinatorClaimExtensionWhenFalse() {
            // given
            var defaultValue = processorConfiguration().coordinatorExtendsClaims();
            var properties = new WorkflowProperties();
            properties.setCoordinatorClaimExtension(false);

            // when
            var result = WorkflowModuleConfigurer.applyProperties(properties, processorConfiguration());

            // then
            assertThat(result.coordinatorExtendsClaims()).isEqualTo(defaultValue);
        }
    }
}
