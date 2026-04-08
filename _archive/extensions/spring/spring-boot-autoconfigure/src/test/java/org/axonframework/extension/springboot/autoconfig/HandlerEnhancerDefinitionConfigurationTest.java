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

package org.axonframework.extension.springboot.autoconfig;

import org.jspecify.annotations.NonNull;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class verifying a {@link HandlerEnhancerDefinition} configurations. For example, that a
 * {@code HandlerEnhancerDefinition} bean will only wrap if there are message handling functions present.
 *
 * @author Steven van Beelen
 */
class HandlerEnhancerDefinitionConfigurationTest {

    private static final AtomicBoolean VERIFY_ENHANCER = new AtomicBoolean(false);

    @BeforeEach
    void setUp() {
        VERIFY_ENHANCER.compareAndSet(true, false);
    }

    @Test
    void handlerEnhancerDefinitionWrapsEventHandler() {
        new ApplicationContextRunner()
                .withUserConfiguration(ContextWithHandlers.class)
                .withPropertyValues("axon.axonserver.enabled=false", "axon.eventstorage.jpa.polling-interval=0")
                .run(context -> {
                    assertThat(context).hasSingleBean(CustomHandlerEnhancerDefinition.class);
                    assertThat(context).hasSingleBean(MyEventHandlingComponent.class);

                    assertTrue(VERIFY_ENHANCER.get());
                });
    }

    @Test
    void handlerEnhancerDefinitionDoesNotWrapInAbsenceOfMessageHandlers() {
        new ApplicationContextRunner()
                .withUserConfiguration(ContextWithoutHandlers.class)
                .withPropertyValues("axon.axonserver.enabled=false", "axon.eventstorage.jpa.polling-interval=0")
                .run(context -> {
                    assertThat(context).hasSingleBean(CustomHandlerEnhancerDefinition.class);

                    assertFalse(VERIFY_ENHANCER.get());
                });
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    private static class ContextWithHandlers {

        @Bean
        public HandlerEnhancerDefinition customHandlerEnhancerDefinition() {
            return new CustomHandlerEnhancerDefinition();
        }

        @Bean
        public MyEventHandlingComponent myEventHandlingComponent() {
            return new MyEventHandlingComponent();
        }
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    private static class ContextWithoutHandlers {

        @Bean
        public HandlerEnhancerDefinition customHandlerEnhancerDefinition() {
            return new CustomHandlerEnhancerDefinition();
        }
    }

    private static class CustomHandlerEnhancerDefinition implements HandlerEnhancerDefinition {

        @Override
        public @NonNull
        <T> MessageHandlingMember<T> wrapHandler(@NonNull MessageHandlingMember<T> original) {
            VERIFY_ENHANCER.set(true);
            return original;
        }
    }

    private static class MyEventHandlingComponent {

        @EventHandler
        public void on(Object someEvent) {

        }
    }
}
