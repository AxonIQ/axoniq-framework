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

package io.axoniq.framework.springboot;

import io.axoniq.framework.springboot.DeadLetterQueueProcessorProperties;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that {@link DeadLetterQueueProcessorProperties} correctly binds
 * DLQ settings from Spring Boot properties under {@code axon.eventhandling.processors.<name>.dlq.*}.
 *
 * @author Mateusz Nowak
 * @since 5.1.0
 */
class DeadLetterQueueProcessorPropertiesTest {

    @SpringBootTest(
            classes = MyContext.class,
            properties = {
                    "axon.axonserver.enabled=false",
                    "axon.postgresql.enabled=false"
            }
    )
    @Nested
    class LoadDefaultProperties {

        @Autowired
        private DeadLetterQueueProcessorProperties dlqProperties;

        @Test
        void defaultsReturnDlqDisabledWithDefaultCacheSize() {
            // when
            DeadLetterQueueProcessorProperties.DlqProcessorSettings settings =
                    dlqProperties.forProcessor("non-existent");

            // then
            assertThat(settings.getDlq().isEnabled()).isFalse();
            assertThat(settings.getDlq().getCache().getSize()).isEqualTo(1024);
        }
    }

    @SpringBootTest(
            classes = MyContext.class,
            properties = {
                    "axon.axonserver.enabled=false",
                    "axon.postgresql.enabled=false",
                    "axon.eventhandling.processors.my-processor.dlq.enabled=true",
                    "axon.eventhandling.processors.my-processor.dlq.cache.size=2048"
            }
    )
    @Nested
    class LoadCustomDlqProperties {

        @Autowired
        private DeadLetterQueueProcessorProperties dlqProperties;

        @Test
        void customPropertiesAreBound() {
            // when
            DeadLetterQueueProcessorProperties.DlqProcessorSettings settings =
                    dlqProperties.forProcessor("my-processor");

            // then
            assertThat(settings.getDlq().isEnabled()).isTrue();
            assertThat(settings.getDlq().getCache().getSize()).isEqualTo(2048);
        }

        @Test
        void unconfiguredProcessorStillReturnsDefaults() {
            // when
            DeadLetterQueueProcessorProperties.DlqProcessorSettings settings =
                    dlqProperties.forProcessor("other-processor");

            // then
            assertThat(settings.getDlq().isEnabled()).isFalse();
            assertThat(settings.getDlq().getCache().getSize()).isEqualTo(1024);
        }
    }

    @SpringBootTest(
            classes = MyContext.class,
            properties = {
                    "axon.axonserver.enabled=false",
                    "axon.postgresql.enabled=false",
                    "axon.eventhandling.processors.processor-a.dlq.enabled=true",
                    "axon.eventhandling.processors.processor-b.dlq.enabled=true",
                    "axon.eventhandling.processors.processor-b.dlq.cache.size=512"
            }
    )
    @Nested
    class LoadMultipleProcessorDlqProperties {

        @Autowired
        private DeadLetterQueueProcessorProperties dlqProperties;

        @Test
        void multipleProcessorsConfiguredIndependently() {
            // when
            DeadLetterQueueProcessorProperties.DlqProcessorSettings settingsA =
                    dlqProperties.forProcessor("processor-a");
            DeadLetterQueueProcessorProperties.DlqProcessorSettings settingsB =
                    dlqProperties.forProcessor("processor-b");

            // then
            assertThat(settingsA.getDlq().isEnabled()).isTrue();
            assertThat(settingsA.getDlq().getCache().getSize()).isEqualTo(1024);

            assertThat(settingsB.getDlq().isEnabled()).isTrue();
            assertThat(settingsB.getDlq().getCache().getSize()).isEqualTo(512);
        }
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableConfigurationProperties(DeadLetterQueueProcessorProperties.class)
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    private static class MyContext {

    }
}
