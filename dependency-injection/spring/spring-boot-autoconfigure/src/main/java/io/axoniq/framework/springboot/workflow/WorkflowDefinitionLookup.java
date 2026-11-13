/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.springboot.workflow;

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;

import java.util.List;
import java.util.Map;

import static io.axoniq.framework.springboot.workflow.WorkflowDefinitionLookupUtils.workflowBeanDefinitions;

/**
 * Workflow definition lookup looking for beans with {@link @Workflow} annotated methods.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class WorkflowDefinitionLookup implements BeanDefinitionRegistryPostProcessor {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowDefinitionLookup.class);

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        if (!(beanFactory instanceof BeanDefinitionRegistry registry)) {
            logger.warn("Given bean factory is not a BeanDefinitionRegistry. Cannot auto-configure workflow handlers");
            return;
        }

        Map<Class<? extends WorkflowContext>, List<String>> workflowBeanDefinitions =
                workflowBeanDefinitions(beanFactory, true);

        if (workflowBeanDefinitions.isEmpty()) {
            return; // don't register an empty configurer; wait until workflows are visible
        }

        String configurerBeanName = "WorkflowModuleConfigurer$$Axon$$WorkflowDefinition";
        AbstractBeanDefinition beanDefinition =
                BeanDefinitionBuilder
                        .genericBeanDefinition(WorkflowModuleConfigurer.class)
                        .addConstructorArgValue(workflowBeanDefinitions)
                        .getBeanDefinition();

        if (registry.containsBeanDefinition(configurerBeanName)) {
            registry.removeBeanDefinition(configurerBeanName);
        }
        registry.registerBeanDefinition(configurerBeanName, beanDefinition);

        logger.debug("Detected {} workflow definition bean{}: {}",
                    workflowBeanDefinitions.size(),
                    workflowBeanDefinitions.size() == 1 ? "" : "s",
                    String.join(", ",
                                workflowBeanDefinitions.values().stream()
                                                       .flatMap(List::stream)
                                                       .toList())
        );
    }


    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {

    }
}
