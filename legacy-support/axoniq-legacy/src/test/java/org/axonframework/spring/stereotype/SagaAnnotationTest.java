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

package org.axonframework.spring.stereotype;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that {@link Saga} keeps its Axon Framework 4 shape: an unchanged Axon Framework 4 saga class
 * still compiles and component-scans as a prototype bean against Axon Framework 5.
 *
 * @author Mateusz Nowak
 */
class SagaAnnotationTest {

    @Nested
    class ComponentScanning {

        @Test
        void aSagaAnnotatedClassBecomesAPrototypeBean() {
            // given / when
            try (var context = new AnnotationConfigApplicationContext(ScanConfig.class)) {
                // then
                BeanDefinition definition = context.getBeanFactory().getBeanDefinition("sagaAnnotationTest.PlainSaga");
                assertThat(definition.isPrototype()).isTrue();
            }
        }
    }

    @Nested
    class SagaStoreAttribute {

        @Test
        void defaultsToAnEmptyString() {
            assertThat(PlainSaga.class.getAnnotation(Saga.class).sagaStore()).isEmpty();
        }

        @Test
        void exposesTheConfiguredValue() {
            assertThat(NamedStoreSaga.class.getAnnotation(Saga.class).sagaStore()).isEqualTo("myStore");
        }
    }

    @Configuration
    @ComponentScan(basePackageClasses = SagaAnnotationTest.class)
    static class ScanConfig {

    }

    @Saga
    static class PlainSaga {

    }

    @Saga(sagaStore = "myStore")
    static class NamedStoreSaga {

    }
}
