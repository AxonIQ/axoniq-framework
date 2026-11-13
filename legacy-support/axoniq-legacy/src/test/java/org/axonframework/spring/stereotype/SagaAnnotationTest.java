/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
