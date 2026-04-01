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
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.engine.configuration.AutoDetectionUtils.MethodWithWorkflowAttributes;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.GenericTypeResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.axoniq.workflow.runtime.engine.configuration.AutoDetectionUtils.workflowMethods;

/**
 * Spring Boot utilities.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
class WorkflowDefinitionLookupUtils {


    private WorkflowDefinitionLookupUtils() {
        // avoid instantiation
    }

    /**
     * Returns a list of beans found in the given {@code register} that contain a handler for the given
     * {@code workflowContextType}. The search will only consider prototype beans (or any other non-singleton or
     * abstract bean definitions) when {@code includePrototypeBeans} is {@code true}.
     *
     * @param workflowContextType   The type of workflow to find handlers for.
     * @param beanFactory           The beanFactory to find these handlers in.
     * @param includePrototypeBeans Whether to include prototype beans.
     * @return A list of bean names with message handlers.
     */
    static List<WorkflowBeanDefinition> handlerBeans(
            @Nonnull Class<? extends WorkflowContext> workflowContextType,
            @Nonnull ConfigurableListableBeanFactory beanFactory,
            boolean includePrototypeBeans) {
        List<WorkflowBeanDefinition> found = new ArrayList<>();
        for (String beanName : beanFactory.getBeanDefinitionNames()) {
            BeanDefinition bd = beanFactory.getBeanDefinition(beanName);

            if (bd.isAutowireCandidate()) {  // excludes unproxied variants of proxied beans
                if (includePrototypeBeans || (bd.isSingleton() && !bd.isAbstract())) {
                    Class<?> beanType = beanFactory.getType(beanName);
                    if (beanType != null) {
                        workflowMethods(beanType, workflowContextType)
                                .collect(Collectors.groupingBy(MethodWithWorkflowAttributes::workflowContextType))
                                .forEach((workflowContextClass, methods) -> {
                                    if (!methods.isEmpty()) {
                                        found.add(new WorkflowBeanDefinition(beanName, beanType, workflowContextClass));
                                    }
                                });
                    }
                }
            }
        }
        return found;
    }

    /**
     * Returns a list of beans found in the given {@code register} that contain a factory for the given
     * {@code workflowContextType}. The search will only consider prototype beans (or any other non-singleton or
     * abstract bean definitions) when {@code includePrototypeBeans} is {@code true}.
     *
     * @param beanFactory           The beanFactory to find these handlers in.
     * @param includePrototypeBeans Whether to include prototype beans.
     * @return A list of bean names with message handlers.
     */
    static List<WorkflowContextFactoryBeanDefinition> factoryBeans(
            @Nonnull ConfigurableListableBeanFactory beanFactory,
            boolean includePrototypeBeans) {
        List<WorkflowContextFactoryBeanDefinition> found = new ArrayList<>();
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
                            found.add(new WorkflowContextFactoryBeanDefinition(beanName,
                                                                               (Class<? extends WorkflowContext>) typeArgument));
                        }
                    }
                }
            }
        }
        return found;
    }

    /**
     * Groups workflow bean definitions by context type.
     *
     * @param workflowBeanDefinitions list of workflow bean definitions.
     * @param beanFactory             bean factory.
     * @return map of context type to workflow bean definitions.
     */
    static Map<Class<? extends WorkflowContext>, List<BeanDefinitionWithWorkflowContextType>> groupByContextType(
            @Nonnull List<WorkflowDefinitionLookupUtils.WorkflowBeanDefinition> workflowBeanDefinitions,
            @Nonnull ConfigurableListableBeanFactory beanFactory
    ) {
        return workflowBeanDefinitions
                .stream()
                .map(wbd -> new BeanDefinitionWithWorkflowContextType(
                        beanFactory.getBeanDefinition(wbd.beanName()),
                        wbd.beanName(),
                        wbd.workflowContextType())
                )
                .collect(Collectors.groupingBy(BeanDefinitionWithWorkflowContextType::workflowContextType));
    }


    /**
     * Represents a workflow bean definition.
     *
     * @param beanName            bean name.
     * @param beanType            bean type.
     * @param workflowContextType type of workflow context.
     */
    @Internal
    record WorkflowBeanDefinition(
            String beanName,
            Class<?> beanType,
            Class<? extends WorkflowContext> workflowContextType
    ) {

    }

    /**
     * Bean definition with workflow context.
     *
     * @param definition          bean definition.
     * @param name                bean name.
     * @param workflowContextType workflow context type.
     */
    @Internal
    record BeanDefinitionWithWorkflowContextType(
            BeanDefinition definition,
            String name,
            Class<? extends WorkflowContext> workflowContextType
    ) {

    }


    /**
     * Bean definition for workflow context factory.
     *
     * @param beanName            name of the bean.
     * @param workflowContextType type of workflow context.
     */
    @Internal
    record WorkflowContextFactoryBeanDefinition(
            String beanName,
            Class<? extends WorkflowContext> workflowContextType
    ) {

    }
}
