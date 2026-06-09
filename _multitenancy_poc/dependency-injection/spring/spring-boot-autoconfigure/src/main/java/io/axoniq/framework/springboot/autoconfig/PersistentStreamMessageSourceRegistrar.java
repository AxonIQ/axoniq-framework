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

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamMessageSource;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import org.axonframework.common.ObjectUtils;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Post-processor that reads {@code axon.axonserver.persistent-streams.*} properties from the application environment
 * and registers a {@link PersistentStreamMessageSource} Spring bean for each configured stream.
 * <p>
 * Each bean is registered under the map key (e.g. {@code axon.axonserver.persistent-streams[my-stream].*} registers a
 * bean named {@code "my-stream"}). This name must match the {@code axon.eventhandling.processors.<name>.source}
 * property so that the subscribing event processor can resolve the correct source.
 *
 * @author Marc Gathier
 * @since 5.2.0
 */
public class PersistentStreamMessageSourceRegistrar implements BeanDefinitionRegistryPostProcessor {

    private static final Logger logger = LoggerFactory.getLogger(PersistentStreamMessageSourceRegistrar.class);

    private final Map<String, AxonServerConfiguration.PersistentStreamSettings> persistentStreams;
    private final PersistentStreamScheduledExecutorBuilder executorBuilder;

    /**
     * Instantiates a {@link PersistentStreamMessageSourceRegistrar}.
     * <p>
     * Reads the {@code axon.axonserver.persistent-streams} map from the given {@code environment} eagerly so that
     * bean definitions can be registered before the Spring context completes its refresh.
     *
     * @param environment     the application configuration environment
     * @param executorBuilder the {@link PersistentStreamScheduledExecutorBuilder} used to construct a
     *                        {@link java.util.concurrent.ScheduledExecutorService} per stream
     */
    public PersistentStreamMessageSourceRegistrar(Environment environment,
                                                  PersistentStreamScheduledExecutorBuilder executorBuilder) {
        Binder binder = Binder.get(environment);
        this.persistentStreams =
                binder.bind(
                              "axon.axonserver.persistent-streams",
                              Bindable.mapOf(String.class, AxonServerConfiguration.PersistentStreamSettings.class)
                      )
                      .orElse(Collections.emptyMap());
        this.executorBuilder = executorBuilder;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(
            BeanDefinitionRegistry beanDefinitionRegistry
    ) throws BeansException {
        Set<String> registeredStreamNames = new HashSet<>();
        persistentStreams.forEach((beanName, settings) -> {
            if (beanDefinitionRegistry.containsBeanDefinition(beanName)) {
                logger.info("Skipping registration of persistent stream '{}': a bean with that name already exists.",
                            beanName);
                return;
            }

            String streamName = ObjectUtils.getOrDefault(settings.getName(), beanName);
            if (!registeredStreamNames.add(streamName)) {
                logger.warn("Duplicate persistent stream name '{}' detected (bean key '{}'). "
                                    + "Multiple entries resolve to the same server-side stream name.",
                            streamName, beanName);
            }

            BeanDefinitionBuilder streamProperties =
                    BeanDefinitionBuilder.genericBeanDefinition(PersistentStreamProperties.class);
            streamProperties.addConstructorArgValue(streamName);
            streamProperties.addConstructorArgValue(settings.getInitialSegmentCount());
            streamProperties.addConstructorArgValue(settings.getSequencingPolicy());
            streamProperties.addConstructorArgValue(settings.getSequencingPolicyParameters());
            streamProperties.addConstructorArgValue(settings.getInitialPosition());
            streamProperties.addConstructorArgValue(settings.getFilter());

            BeanDefinitionBuilder beanDefinition =
                    BeanDefinitionBuilder.genericBeanDefinition(PersistentStreamMessageSource.class);
            beanDefinition.addConstructorArgValue(streamName);
            beanDefinition.addConstructorArgValue(new RuntimeBeanReference(AxonServerConnectionManager.class));
            beanDefinition.addConstructorArgValue(new RuntimeBeanReference(AxonServerConfiguration.class));
            beanDefinition.addConstructorArgValue(new RuntimeBeanReference(EventConverter.class));
            beanDefinition.addConstructorArgValue(streamProperties.getBeanDefinition());
            beanDefinition.addConstructorArgValue(executorBuilder.build(settings.getThreadCount(), streamName));
            beanDefinition.addConstructorArgValue(new RuntimeBeanReference(UnitOfWorkFactory.class));
            beanDefinition.addConstructorArgValue(settings.getBatchSize());

            beanDefinitionRegistry.registerBeanDefinition(beanName, beanDefinition.getBeanDefinition());
        });
    }

    @Override
    public void postProcessBeanFactory(
            ConfigurableListableBeanFactory configurableListableBeanFactory
    ) throws BeansException {
        // No actions needed here
    }
}
