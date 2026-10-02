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
import org.junit.jupiter.api.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The workflow properties are the application-facing surface of the {@code axon.workflow} prefix, so the binding of
 * every property is asserted here: a renamed prefix or setter still starts the application, silently on the default.
 */
class WorkflowPropertiesTest {

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner().withUserConfiguration(TestConfiguration.class);

    @Test
    void initialSegmentCountBindsFromTheApplicationProperties() {
        contextRunner.withPropertyValues("axon.workflow.initial-segment-count=16")
                     .run(context -> assertThat(
                             context.getBean(WorkflowProperties.class).getInitialSegmentCount()
                     ).isEqualTo(16));
    }

    @Test
    void batchSizeBindsFromTheApplicationProperties() {
        contextRunner.withPropertyValues("axon.workflow.batch-size=50")
                     .run(context -> assertThat(
                             context.getBean(WorkflowProperties.class).getBatchSize()
                     ).isEqualTo(50));
    }

    @Test
    void threadCountBindsFromTheApplicationProperties() {
        contextRunner.withPropertyValues("axon.workflow.thread-count=4")
                     .run(context -> assertThat(
                             context.getBean(WorkflowProperties.class).getThreadCount()
                     ).isEqualTo(4));
    }

    @Test
    void tokenClaimIntervalBindsFromTheApplicationProperties() {
        contextRunner.withPropertyValues("axon.workflow.token-claim-interval=2500")
                     .run(context -> assertThat(
                             context.getBean(WorkflowProperties.class).getTokenClaimInterval()
                     ).isEqualTo(2500L));
    }

    @Test
    void claimExtensionThresholdBindsFromTheApplicationProperties() {
        contextRunner.withPropertyValues("axon.workflow.claim-extension-threshold=7500")
                     .run(context -> assertThat(
                             context.getBean(WorkflowProperties.class).getClaimExtensionThreshold()
                     ).isEqualTo(7500L));
    }

    @Test
    void coordinatorClaimExtensionBindsFromTheApplicationProperties() {
        contextRunner.withPropertyValues("axon.workflow.coordinator-claim-extension=true")
                     .run(context -> assertThat(context.getBean(WorkflowProperties.class)
                                                       .getCoordinatorClaimExtension()).isTrue());
    }

    @Test
    void everyPropertyDefaultsToTheSameValueAsEventProcessorPropertiesProcessorSettings() {
        contextRunner.run(context -> {
            var properties = context.getBean(WorkflowProperties.class);
            assertThat(properties.getInitialSegmentCount())
                    .as("Matches EventProcessorProperties.ProcessorSettings' own default, so an application that "
                                + "sets none of these properties gets identical processor behavior.")
                    .isEqualTo(16);
            assertThat(properties.getBatchSize()).isEqualTo(1);
            assertThat(properties.getThreadCount()).isEqualTo(4);
            assertThat(properties.getTokenClaimInterval()).isEqualTo(5000L);
            assertThat(properties.getClaimExtensionThreshold()).isEqualTo(5000L);
            assertThat(properties.getCoordinatorClaimExtension()).isFalse();
        });
    }

    @Configuration
    @EnableConfigurationProperties(WorkflowProperties.class)
    static class TestConfiguration {

    }
}
