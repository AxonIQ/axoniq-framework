/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.eventhandling.scheduling;

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventhandling.scheduling.java.SimpleEventScheduler;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.junit.jupiter.api.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that the {@link EventSchedulerLifecycleConfigurationEnhancer} shuts down the event schedulers
 * of a configuration with the application.
 *
 * @author Jakob Hatzl
 */
class EventSchedulerLifecycleConfigurationEnhancerTest {

    private ScheduledExecutorService scheduledExecutorService;
    private SimpleEventScheduler eventScheduler;

    @BeforeEach
    void setUp() {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        eventScheduler = SimpleEventScheduler.builder()
                                             .scheduledExecutorService(scheduledExecutorService)
                                             .eventSink(new SimpleEventBus())
                                             .unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY)
                                             .build();
    }

    @AfterEach
    void tearDown() {
        scheduledExecutorService.shutdownNow();
    }

    @Test
    void anEventSchedulerOfTheConfigurationIsShutDownWithTheApplication() {
        // given
        AxonConfiguration configuration =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> cr.registerComponent(EventScheduler.class,
                                                                                 c -> eventScheduler))
                                   .start();
        assertThat(scheduledExecutorService.isShutdown()).isFalse();

        // when
        configuration.shutdown();

        // then
        assertThat(scheduledExecutorService.isShutdown()).isTrue();
    }

    @Test
    void anEventSchedulerRegisteredUnderItsImplementationTypeIsShutDownToo() {
        // given
        AxonConfiguration configuration =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> cr.registerComponent(SimpleEventScheduler.class,
                                                                                 c -> eventScheduler))
                                   .start();

        // when
        configuration.shutdown();

        // then
        assertThat(scheduledExecutorService.isShutdown()).isTrue();
    }

    @Test
    void anApplicationDisablingTheEnhancerShutsDownItsEventSchedulerItself() {
        // given
        AxonConfiguration configuration =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> cr.registerComponent(EventScheduler.class,
                                                                                 c -> eventScheduler)
                                                              .disableEnhancer(
                                                                      EventSchedulerLifecycleConfigurationEnhancer.class
                                                              ))
                                   .start();

        // when
        configuration.shutdown();

        // then
        assertThat(scheduledExecutorService.isShutdown()).isFalse();
    }
}
