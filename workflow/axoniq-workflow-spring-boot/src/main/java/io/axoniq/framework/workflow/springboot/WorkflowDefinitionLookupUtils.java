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

import io.axoniq.framework.workflow.configuration.AutoDetectionUtils.MethodWithWorkflowAttributes;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import org.axonframework.common.annotation.Internal;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.GenericTypeResolver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.axoniq.framework.workflow.configuration.AutoDetectionUtils.workflowMethods;

/**
 * Helper utilities for workflow definition lookup.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
class WorkflowDefinitionLookupUtils {


    private WorkflowDefinitionLookupUtils() {
        // avoid instantiation
    }

    /**
     * Returns a map of bean names found in the given {@code beanFactory} that contain a handler for the given
     * {@code workflowContextType} grouped by the class extending the {@link WorkflowContext}. The search will only
     * consider prototype beans (or any other non-singleton or abstract bean definitions) when
     * {@code includePrototypeBeans} is {@code true}.
     *
     * @param workflowContextType   The type of workflow to find handlers for.
     * @param beanFactory           The beanFactory to find these handlers in.
     * @param includePrototypeBeans Whether to include prototype beans.
     * @return A map keyed by the workflow context type, containing a list of bean definitions.
     */
    static Map<Class<? extends WorkflowContext>, List<String>> workflowBeanDefinitions(
            Class<? extends WorkflowContext> workflowContextType,
            ConfigurableListableBeanFactory beanFactory,
            boolean includePrototypeBeans) {

        Map<Class<? extends WorkflowContext>, List<String>> found = new HashMap<>();

        for (String beanName : beanFactory.getBeanDefinitionNames()) {
            BeanDefinition bd = beanFactory.getBeanDefinition(beanName);

            if (bd.isAutowireCandidate()) {  // excludes unproxied variants of proxied beans
                if (includePrototypeBeans || (bd.isSingleton() && !bd.isAbstract())) {
                    Class<?> beanType = beanFactory.getType(beanName);
                    if (beanType != null) {
                        workflowMethods(beanType, workflowContextType)
                                .map(MethodWithWorkflowAttributes::workflowContextType)
                                .forEach(workflowContextClass -> {
                                    found.computeIfAbsent(workflowContextClass, k -> new ArrayList<>()).add(beanName);
                                });
                    }
                }
            }
        }
        return found;
    }

    /**
     * Returns a map of workflow context factory bean names found in the given {@code beanFactory} keyed by the workflow
     * context type. The search will only consider prototype beans (or any other non-singleton or abstract bean
     * definitions) when {@code includePrototypeBeans} is {@code true}.
     *
     * @param beanFactory           The beanFactory to find these handlers in.
     * @param includePrototypeBeans Whether to include prototype beans.
     * @return A list of bean names with message handlers.
     */
    static Map<Class<? extends WorkflowContext>, String> workflowContextFactoryBeans(
            ConfigurableListableBeanFactory beanFactory,
            boolean includePrototypeBeans) {

        Map<Class<? extends WorkflowContext>, String> found = new HashMap<>();

        for (String beanName : beanFactory.getBeanDefinitionNames()) {
            BeanDefinition bd = beanFactory.getBeanDefinition(beanName);
            if (bd.isAutowireCandidate()) {  // excludes unproxied variants of proxied beans
                if (includePrototypeBeans || (bd.isSingleton() && !bd.isAbstract())) {
                    Class<?> beanType = beanFactory.getType(beanName);
                    if (beanType != null && WorkflowContextFactory.class.isAssignableFrom(beanType)) {
                        Class<?> typeArgument = GenericTypeResolver.resolveTypeArgument(beanType,
                                                                                        WorkflowContextFactory.class);
                        if (typeArgument != null && WorkflowContext.class.isAssignableFrom(typeArgument)) {
                            //noinspection unchecked
                            found.put((Class<? extends WorkflowContext>) typeArgument, beanName);
                        }
                    }
                }
            }
        }
        return found;
    }
}
