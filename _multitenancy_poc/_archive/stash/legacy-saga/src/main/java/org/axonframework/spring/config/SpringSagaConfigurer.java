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

package org.axonframework.spring.config;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * A {@link ConfigurationEnhancer} implementation that configures a Saga based on configuration found in the Application
 * Context.
 *
 * @author Allard Buijze
 * @since 4.6.0
 */
// TODO #3097 Fix as part of referred to issue
public class SpringSagaConfigurer implements ConfigurationEnhancer, ApplicationContextAware {

    private final Class<?> sagaType;
    private String sagaStore;
    private ApplicationContext applicationContext;

    /**
     * Initialize the Saga for given {@code sagaType}.
     *
     * @param sagaType The type of Saga to configure.
     */
    public SpringSagaConfigurer(Class<?> sagaType) {
        this.sagaType = sagaType;
    }

    /**
     * Sets the bean name of the {@link SagaStore} to configure.
     *
     * @param sagaStore the bean name of the {@link SagaStore} to configure.
     */
    public void setSagaStore(String sagaStore) {
        this.sagaStore = sagaStore;
    }

    @Override
    public void enhance(ComponentRegistry registry) {
//        configurer.eventProcessing()
//                  .registerSaga(sagaType,
//                                sagaConfigurer -> {
//                                    if (sagaStore != null && !"".equals(sagaStore)) {
//                                        noinspection unchecked
//                                        sagaConfigurer.configureSagaStore(
//                                                c -> applicationContext.getBean(sagaStore, SagaStore.class)
//                                        );
//                                    }
//                                });
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
