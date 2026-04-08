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

package org.axonframework.spring.saga;

import org.axonframework.modelling.saga.ResourceInjector;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * ResourceInjector implementation that injects Saga instances with resources available from the Spring Application
 * context the injector is registered in.
 * <p/>
 * Resources need to be annotated with a Spring-compatible auto-wiring annotation, such as
 * {@link org.springframework.beans.factory.annotation.Autowired @Autowired} or JSR-250's
 * {@link jakarta.annotation.Resource}.
 * <p/>
 * Note: make sure that the Spring context also declares {@code &lt;context:annotation-config /&gt;} or an
 * {@code AutowiredAnnotationBeanPostProcessor}. See the Spring documentation for more information.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public class SpringResourceInjector implements ResourceInjector, ApplicationContextAware {

    private AutowireCapableBeanFactory autowireCapableBeanFactory;

    @Override
    public void injectResources(Object saga) {
        autowireCapableBeanFactory.autowireBeanProperties(saga, AutowireCapableBeanFactory.AUTOWIRE_NO, false);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.autowireCapableBeanFactory = applicationContext.getAutowireCapableBeanFactory();
    }
}
