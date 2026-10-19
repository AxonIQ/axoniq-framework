/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.extension.spring.config;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;
import org.axonframework.spring.stereotype.Saga;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that {@link MessageHandlerLookup} keeps an Axon Framework 4 {@link Saga @Saga} out of the
 * plain event handling components.
 * <p>
 * A Saga carries {@link SagaEventHandler @SagaEventHandler} methods, which are meta-annotated with
 * {@link org.axonframework.messaging.eventhandling.annotation.EventHandler @EventHandler} and therefore with
 * {@link org.axonframework.messaging.core.annotation.MessageHandler @MessageHandler}. The lookup would happily wire a
 * Saga up as a plain annotated event handling component in addition to its declarative Saga component if
 * {@link Saga @Saga} did not make the bean a prototype. This test pins that {@code @Saga} produces the prototype scope
 * the lookup relies on.
 *
 * @author Mateusz Nowak
 */
class SagaMessageHandlerLookupTest {

    @Test
    void excludesAPrototypeScopedSaga() {
        // given - the same Saga type as a prototype and as a singleton bean
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        sagaBean(beanFactory, "prototypeSaga", BeanDefinition.SCOPE_PROTOTYPE);
        sagaBean(beanFactory, "singletonSaga", BeanDefinition.SCOPE_SINGLETON);

        // when
        List<String> found = MessageHandlerLookup.messageHandlerBeans(EventMessage.class, beanFactory, false);

        // then - the singleton proves the handler is detected; only the scope keeps the Saga out
        assertThat(found).containsExactly("singletonSaga");
    }

    @Test
    void includesAPrototypeScopedSagaWhenPrototypeBeansAreRequested() {
        // given
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        sagaBean(beanFactory, "prototypeSaga", BeanDefinition.SCOPE_PROTOTYPE);

        // when
        List<String> found = MessageHandlerLookup.messageHandlerBeans(EventMessage.class, beanFactory, true);

        // then
        assertThat(found).containsExactly("prototypeSaga");
    }

    private static void sagaBean(DefaultListableBeanFactory beanFactory, String beanName, String scope) {
        beanFactory.registerBeanDefinition(beanName,
                                           BeanDefinitionBuilder.genericBeanDefinition(SimpleSaga.class)
                                                                .setScope(scope)
                                                                .getBeanDefinition());
    }

    @Saga
    static class SimpleSaga {

        @StartSaga
        @SagaEventHandler(associationProperty = "id")
        void on(SagaStarted event) {
            // Intentionally empty; the Saga only needs a handler for the lookup to consider it.
        }
    }

    record SagaStarted(String id) {

    }
}
