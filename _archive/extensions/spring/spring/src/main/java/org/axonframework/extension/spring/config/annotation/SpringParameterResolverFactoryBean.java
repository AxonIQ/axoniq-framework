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

package org.axonframework.extension.spring.config.annotation;

import org.axonframework.messaging.core.annotation.ClasspathParameterResolverFactory;
import org.axonframework.messaging.core.annotation.MultiParameterResolverFactory;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.ArrayList;
import java.util.List;

/**
 * Spring factory bean that creates a {@link ParameterResolverFactory} instance that is capable of resolving parameter
 * values as Spring Beans, in addition to the default behavior defined by Axon.
 *
 * @author Allard Buijze
 * @see SpringBeanParameterResolverFactory
 * @see SpringBeanDependencyResolverFactory
 * @see ClasspathParameterResolverFactory
 * @since 2.3.1
 */
public class SpringParameterResolverFactoryBean implements
        FactoryBean<ParameterResolverFactory>,
        BeanClassLoaderAware,
        InitializingBean,
        ApplicationContextAware {

    private final List<ParameterResolverFactory> factories = new ArrayList<>();

    private ClassLoader classLoader;
    private ApplicationContext applicationContext;

    @Override
    public ParameterResolverFactory getObject() {
        return MultiParameterResolverFactory.ordered(factories);
    }

    @Override
    public Class<?> getObjectType() {
        return ParameterResolverFactory.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void afterPropertiesSet() {
        factories.add(ClasspathParameterResolverFactory.forClassLoader(classLoader));
        factories.add(new SpringBeanDependencyResolverFactory(applicationContext));
        factories.add(new SpringBeanParameterResolverFactory(applicationContext));
    }

    /**
     * Defines any additional parameter resolver factories that need to be used to resolve parameters. By default, the
     * ParameterResolverFactories found on the classpath, as well as a SpringBeanParameterResolverFactory are
     * registered.
     *
     * @param additionalFactories The extra factories to register
     * @see SpringBeanParameterResolverFactory
     * @see SpringBeanDependencyResolverFactory
     * @see ClasspathParameterResolverFactory
     */
    public void setAdditionalFactories(List<ParameterResolverFactory> additionalFactories) {
        this.factories.addAll(additionalFactories);
    }

    @Override
    public void setBeanClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
