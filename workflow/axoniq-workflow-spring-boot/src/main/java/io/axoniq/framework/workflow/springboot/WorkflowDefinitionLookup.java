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
package io.axoniq.framework.workflow.springboot;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
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

import static io.axoniq.framework.workflow.springboot.WorkflowDefinitionLookupUtils.workflowBeanDefinitions;
import static io.axoniq.framework.workflow.springboot.WorkflowDefinitionLookupUtils.workflowContextFactoryBeans;

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

        Map<Class<? extends WorkflowContext>, String> factoryBeanDefinitions =
                workflowContextFactoryBeans(beanFactory, false);

        String configurerBeanName = "WorkflowModuleConfigurer$$Axon$$WorkflowDefinition";
        AbstractBeanDefinition beanDefinition =
                BeanDefinitionBuilder
                        .genericBeanDefinition(WorkflowModuleConfigurer.class)
                        .addConstructorArgValue(factoryBeanDefinitions)
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
