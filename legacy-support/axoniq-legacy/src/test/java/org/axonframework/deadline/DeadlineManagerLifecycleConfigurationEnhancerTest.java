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

package org.axonframework.deadline;

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that the {@link DeadlineManagerLifecycleConfigurationEnhancer} shuts down the deadline managers
 * of a configuration with the application.
 *
 * @author Jakob Hatzl
 */
class DeadlineManagerLifecycleConfigurationEnhancerTest {

    private final List<String> timeline = new CopyOnWriteArrayList<>();
    private final RecordingDeadlineManager deadlineManager = new RecordingDeadlineManager("manager", timeline);

    @Test
    void aDeadlineManagerOfTheConfigurationIsShutDownWithTheApplication() {
        // given
        AxonConfiguration configuration =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> cr.registerComponent(DeadlineManager.class,
                                                                                 c -> deadlineManager))
                                   .start();
        assertThat(timeline).isEmpty();

        // when
        configuration.shutdown();

        // then
        assertThat(timeline).containsExactly("manager:shutdown");
    }

    @Test
    void aDeadlineManagerRegisteredUnderItsImplementationTypeIsShutDownToo() {
        // given
        AxonConfiguration configuration =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> cr.registerComponent(RecordingDeadlineManager.class,
                                                                                 c -> deadlineManager))
                                   .start();

        // when
        configuration.shutdown();

        // then
        assertThat(timeline).containsExactly("manager:shutdown");
    }

    @Test
    void anApplicationDisablingTheEnhancerShutsDownItsDeadlineManagerItself() {
        // given
        AxonConfiguration configuration =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> cr.registerComponent(DeadlineManager.class,
                                                                                 c -> deadlineManager)
                                                              .disableEnhancer(
                                                                      DeadlineManagerLifecycleConfigurationEnhancer.class
                                                              ))
                                   .start();

        // when
        configuration.shutdown();

        // then
        assertThat(timeline).isEmpty();
    }
}
