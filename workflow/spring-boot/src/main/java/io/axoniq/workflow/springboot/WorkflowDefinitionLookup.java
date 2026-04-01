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
package io.axoniq.workflow.springboot;

import io.axoniq.workflow.runtime.api.WorkflowContext;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;

import java.util.List;

import static io.axoniq.workflow.springboot.SpringUtils.*;

/**
 * Workflow definition lookup looking for beans with {@link @Workflow} annotated methods.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class WorkflowDefinitionLookup implements BeanDefinitionRegistryPostProcessor {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowDefinitionLookup.class);

    @Override
    public void postProcessBeanFactory(@Nonnull ConfigurableListableBeanFactory beanFactory) throws BeansException {
        if (!(beanFactory instanceof BeanDefinitionRegistry)) {
            logger.warn("Given bean factory is not a BeanDefinitionRegistry. Cannot auto-configure workflow handlers");
            return;
        }

        String configurerBeanName = "WorkflowModuleConfigurer$$Axon$$WorkflowDefinition";
        if (beanFactory.containsBeanDefinition(configurerBeanName)) {
            logger.info("Workflow handler configurer already available. Skipping configuration");
            return;
        }

        List<SpringUtils.WorkflowContextFactoryBeanDefinition> workflowFactoryBeanDefinitions = factoryBeans(beanFactory,
                                                                                                             false);
        List<SpringUtils.WorkflowBeanDefinition> found = handlerBeans(WorkflowContext.class, beanFactory, false);
        if (!found.isEmpty()) {
            List<SpringUtils.WorkflowBeanDefinition> sortedFound = sortByOrder(found, beanFactory);
            AbstractBeanDefinition beanDefinition =
                    BeanDefinitionBuilder
                            .genericBeanDefinition(WorkflowModuleConfigurer.class)
                            .addConstructorArgValue(workflowFactoryBeanDefinitions)
                            .addConstructorArgValue(sortedFound)
                            .getBeanDefinition();
            ((BeanDefinitionRegistry) beanFactory).registerBeanDefinition(configurerBeanName, beanDefinition);
        }
        logger.info("Detected {} workflow definition bean{}: {}",
                    found.size(),
                    found.size() == 1 ? "" : "s",
                    String.join(", ", found.stream().map(WorkflowBeanDefinition::beanName).toList())
        );
    }


    @Override
    public void postProcessBeanDefinitionRegistry(@Nonnull BeanDefinitionRegistry registry) throws BeansException {
        // No action required.
    }
}
