/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.springboot.workflow;

import io.axoniq.framework.springboot.WorkflowProperties;
import io.axoniq.framework.springboot.autoconfig.WorkflowAutoConfiguration;
import io.axoniq.framework.workflow.configuration.WorkflowSnapshotConfiguration;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
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
    void checkpointIntervalBindsFromTheApplicationProperties() {
        contextRunner.withPropertyValues("axon.workflow.checkpoint-interval=1000")
                     .run(context -> assertThat(
                             context.getBean(WorkflowProperties.class).getCheckpointInterval()
                     ).isEqualTo(1000L));
    }

    @Test
    void checkpointIntervalDefaultsToRequestingACheckpointAfterEveryEvent() {
        contextRunner.run(context -> assertThat(
                context.getBean(WorkflowProperties.class).getCheckpointInterval()
        ).isZero());
    }

    @Test
    void indexBindsFromTheApplicationProperties() {
        contextRunner.withPropertyValues("axon.workflow.index=jpa")
                     .run(context -> assertThat(context.getBean(WorkflowProperties.class).getIndex())
                             .isEqualTo(WorkflowProperties.WorkflowIndexType.JPA));
    }

    @Test
    void indexDefaultsToKeepingNoIndex() {
        contextRunner.run(context -> assertThat(context.getBean(WorkflowProperties.class).getIndex())
                .isEqualTo(WorkflowProperties.WorkflowIndexType.NONE));
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

    @Nested
    class Snapshots {

        @Test
        void snapshotsAreNeitherEnabledNorDisabledWithFiftyEventsPerSnapshotByDefault() {
            contextRunner.run(context -> {
                // when
                var snapshots = context.getBean(WorkflowProperties.class).getSnapshots();

                // then
                assertThat(snapshots.getEnabled()).isNull();
                assertThat(snapshots.getRunningWorkflows().getAfterEvents()).isEqualTo(50);
                assertThat(snapshots.getWorkflowState().getAfterEvents()).isEqualTo(50);
            });
        }

        @Test
        void snapshotPropertiesBindFromTheApplicationProperties() {
            contextRunner.withPropertyValues(
                    "axon.workflow.snapshots.enabled=true",
                    "axon.workflow.snapshots.running-workflows.after-events=250",
                    "axon.workflow.snapshots.workflow-state.after-events=75"
            ).run(context -> {
                // when
                var snapshots = context.getBean(WorkflowProperties.class).getSnapshots();

                // then
                assertThat(snapshots.getEnabled()).isTrue();
                assertThat(snapshots.getRunningWorkflows().getAfterEvents()).isEqualTo(250);
                assertThat(snapshots.getWorkflowState().getAfterEvents()).isEqualTo(75);
            });
        }

        @ParameterizedTest
        @ValueSource(strings = {"true", "yes", "on"})
        void everyBooleanSpellingOfTrueEnablesSnapshots(String enabled) {
            contextRunner.withPropertyValues("axon.workflow.snapshots.enabled=" + enabled).run(context -> {
                // when
                var snapshots = context.getBean(WorkflowProperties.class).getSnapshots();

                // then
                assertThat(snapshots.getEnabled()).isTrue();
            });
        }

        @Test
        void disabledSnapshotsBindFromTheApplicationProperties() {
            contextRunner.withPropertyValues("axon.workflow.snapshots.enabled=false").run(context -> {
                // when
                var snapshots = context.getBean(WorkflowProperties.class).getSnapshots();

                // then
                assertThat(snapshots.getEnabled()).isFalse();
            });
        }

        @Nested
        @SuppressWarnings("deprecation")
        class FormerBooleanAccessors {

            @Test
            void isEnabledTellsWhetherSnapshotsAreExplicitlyEnabled() {
                // given
                var unset = new WorkflowProperties.SnapshotProperties();
                var enabled = new WorkflowProperties.SnapshotProperties();
                var disabled = new WorkflowProperties.SnapshotProperties();

                // when
                enabled.setEnabled(true);
                disabled.setEnabled(false);

                // then
                assertThat(unset.isEnabled()).isFalse();
                assertThat(enabled.isEnabled()).isTrue();
                assertThat(disabled.isEnabled()).isFalse();
                assertThat(disabled.getEnabled()).isFalse();
            }
        }
    }

    @Nested
    class SnapshotConfiguration {

        private final ApplicationContextRunner autoConfigurationRunner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(WorkflowAutoConfiguration.class));

        @Test
        void isNeitherEnabledNorDisabledWithoutTheEnabledProperty() {
            autoConfigurationRunner.run(context -> {
                // when
                var snapshots = context.getBean(WorkflowSnapshotConfiguration.class);

                // then
                assertThat(snapshots.enabled()).isFalse();
                assertThat(snapshots.disabled()).isFalse();
            });
        }

        @Test
        void isEnabledByTheEnabledProperty() {
            autoConfigurationRunner.withPropertyValues("axon.workflow.snapshots.enabled=yes").run(context -> {
                // when
                var snapshots = context.getBean(WorkflowSnapshotConfiguration.class);

                // then
                assertThat(snapshots.enabled()).isTrue();
                assertThat(snapshots.disabled()).isFalse();
            });
        }

        @Test
        void isDisabledByTheEnabledProperty() {
            autoConfigurationRunner.withPropertyValues("axon.workflow.snapshots.enabled=false").run(context -> {
                // when
                var snapshots = context.getBean(WorkflowSnapshotConfiguration.class);

                // then
                assertThat(snapshots.enabled()).isFalse();
                assertThat(snapshots.disabled()).isTrue();
            });
        }

        @Test
        void isTheOnlySnapshotSettingTheAutoConfigurationProvides() {
            autoConfigurationRunner.withPropertyValues("axon.workflow.snapshots.enabled=true").run(context -> {
                // when / then - no SnapshotPolicy bean competes with the configuration
                assertThat(context).hasSingleBean(WorkflowSnapshotConfiguration.class)
                                   .doesNotHaveBean(SnapshotPolicy.class);
            });
        }
    }

    @Configuration
    @EnableConfigurationProperties(WorkflowProperties.class)
    static class TestConfiguration {

    }
}
