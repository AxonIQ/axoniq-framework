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
import org.axonframework.common.ObjectUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.GenericTypeResolver;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.annotation.OrderUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.axoniq.workflow.runtime.engine.configuration.AutoDetectionUtils.workflowMethods;

/**
 * Spring Boot utilities.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class SpringUtils {


    private SpringUtils() {
        // avoid instantiation
    }

    /**
     * Sorts the given {@code found} bean references based on the value in the {@link Order} annotation on these beans.
     * <p>
     * This method uses the given {@code beanFactory} to find the value of the {@code Order} annotation. In absence of
     * this annotation, the order defaults to {@link Ordered#LOWEST_PRECEDENCE}.
     *
     * @param found       The bean references of components containing message handling functions.
     * @param beanFactory The bean factory used to find the {@link Order} annotation value.
     * @return A new {@link List} of sorted bean references, according to the value of the {@link Order} annotation
     * value on the beans.
     */
    public static List<WorkflowBeanDefinition> sortByOrder(List<WorkflowBeanDefinition> found,
                                                           ConfigurableListableBeanFactory beanFactory) {
        return found.stream()
                    .sorted(java.util.Comparator.comparingInt(beanRef ->
                            OrderUtils.getOrder(
                                    ObjectUtils.getOrDefault(beanFactory.getType(beanRef.beanName), Object.class),
                                    Ordered.LOWEST_PRECEDENCE
                            )
                    ))
                    .collect(Collectors.toList());
    }

    /**
     * Returns a list of beans found in the given {@code register} that contain a handler for the given
     * {@code workflowContextType}. The search will only consider prototype beans (or any other non-singleton or
     * abstract bean definitions) when {@code includePrototypeBeans} is {@code true}.
     *
     * @param workflowContextType   The type of workflow to find handlers for.
     * @param registry              The registry to find these handlers in.
     * @param includePrototypeBeans Whether to include prototype beans.
     * @return A list of bean names with message handlers.
     */
    public static List<WorkflowBeanDefinition> handlerBeans(Class<? extends WorkflowContext> workflowContextType,
                                                            ConfigurableListableBeanFactory registry,
                                                            boolean includePrototypeBeans) {
        List<WorkflowBeanDefinition> found = new ArrayList<>();
        for (String beanName : registry.getBeanDefinitionNames()) {
            BeanDefinition bd = registry.getBeanDefinition(beanName);

            if (bd.isAutowireCandidate()) {  // excludes unproxied variants of proxied beans
                if (includePrototypeBeans || (bd.isSingleton() && !bd.isAbstract())) {
                    Class<?> beanType = registry.getType(beanName);
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
     * @param registry              The registry to find these handlers in.
     * @param includePrototypeBeans Whether to include prototype beans.
     * @return A list of bean names with message handlers.
     */
    public static List<WorkflowContextFactoryBeanDefinition> factoryBeans(ConfigurableListableBeanFactory registry,
                                                                          boolean includePrototypeBeans) {
        List<WorkflowContextFactoryBeanDefinition> found = new ArrayList<>();
        for (String beanName : registry.getBeanDefinitionNames()) {
            BeanDefinition bd = registry.getBeanDefinition(beanName);
            if (bd.isAutowireCandidate()) {  // excludes unproxied variants of proxied beans
                if (includePrototypeBeans || (bd.isSingleton() && !bd.isAbstract())) {
                    Class<?> beanType = registry.getType(beanName);
                    if (beanType != null && WorkflowContextFactory.class.isAssignableFrom(beanType)) {
                        Class<?> typeArgument = GenericTypeResolver.resolveTypeArgument(beanType, WorkflowContextFactory.class);
                        if (typeArgument != null && WorkflowContext.class.isAssignableFrom(typeArgument)) {
                            found.add(new WorkflowContextFactoryBeanDefinition(beanName, (Class<? extends WorkflowContext>) typeArgument));
                        }
                    }
                }
            }
        }
        return found;
    }

    /**
     * Represents a workflow bean definition.
     *
     * @param beanName            bean name.
     * @param beanType            bean type.
     * @param workflowContextType type of workflow context.
     */
    public record WorkflowBeanDefinition(
            String beanName,
            Class<?> beanType,
            Class<? extends WorkflowContext> workflowContextType
    ) {

    }

    /**
     * Bean definition for workflow context factory.
     *
     * @param beanName            name of the bean.
     * @param workflowContextType type of workflow context.
     */
    public record WorkflowContextFactoryBeanDefinition(
            String beanName,
            Class<? extends WorkflowContext> workflowContextType
    ) {

    }
}
