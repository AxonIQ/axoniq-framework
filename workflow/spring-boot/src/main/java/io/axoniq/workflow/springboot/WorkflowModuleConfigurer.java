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
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModuleEnhancer;
import io.axoniq.workflow.runtime.engine.execution.DSLAdoptingExecutionFactory;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


@Internal
@RegistrationScope("Don't copy this enhancer in order to avoid cyclic module build in Spring Boot.")
public class WorkflowModuleConfigurer implements ConfigurationEnhancer, ApplicationContextAware {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowModuleConfigurer.class);
    private final List<SpringUtils.WorkflowBeanDefinition> workflowDefinitionBeansRefs;
    private final Map<Class<? extends WorkflowContext>, String> workflowContextFactories;
    private @Nullable ApplicationContext applicationContext;


    /**
     * Creates a new configurer responsible for registration of all found workflows definitions.
     */
    public WorkflowModuleConfigurer(
            List<SpringUtils.WorkflowContextFactoryBeanDefinition> factories,
            List<SpringUtils.WorkflowBeanDefinition> workflowDefinitionBeansRefs
    ) {
        this.workflowDefinitionBeansRefs = workflowDefinitionBeansRefs;
        this.workflowContextFactories = factories.stream().collect(
                Collectors.toMap(
                        SpringUtils.WorkflowContextFactoryBeanDefinition::workflowContextType,
                        SpringUtils.WorkflowContextFactoryBeanDefinition::beanName
                )
        );
    }

    @Override
    public void enhance(@NonNull ComponentRegistry registry) {

        Objects.requireNonNull(applicationContext, "ApplicationContext must not be null");
        groupBeanDefinitionsByWorkflowContextType()
                .forEach((workflowContextType, beanDefs) -> {

                    var factoryName = workflowContextFactories.get(workflowContextType);
                    if (factoryName != null) {
                        var moduleBuilder = WorkflowModule
                                .usingContext((Class<WorkflowContext>) workflowContextType)
                                .workflowContextFactory(
                                        c -> (WorkflowContextFactory<WorkflowContext>) applicationContext.getBean(
                                                factoryName))
                                .workflowExecutionFactory(c -> new DSLAdoptingExecutionFactory<>(workflowContextType));

                        if (!beanDefs.isEmpty()) {
                            WorkflowModule<?> module = null;
                            for (var beanDef : beanDefs) {
                                module = moduleBuilder
                                        .definitions(d -> d.autodetected(
                                                             c -> applicationContext.getBean(beanDef.name()),
                                                             ((Class<WorkflowContext>) workflowContextType)
                                                     )
                                        );
                            }
                            registry.registerEnhancer(new WorkflowModuleEnhancer(module));
                        }
                    } else {
                        logger.error(
                                "Detected workflow definition in '{}' without a WorkflowContextFactory for the workflow type {}.",
                                String.join(",",
                                            beanDefs.stream().map(BeanDefinitionWithWorkflowContextType::name).toList()
                                ),
                                workflowContextType.getSimpleName()
                        );
                    }
                });
    }

    private Map<Class<? extends WorkflowContext>, List<BeanDefinitionWithWorkflowContextType>> groupBeanDefinitionsByWorkflowContextType() {

        // We need access to the BeanFactory, so cast to ConfigurableApplicationContext
        var beanFactory = Objects.requireNonNull((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        return workflowDefinitionBeansRefs
                .stream()
                .map(wbd -> new BeanDefinitionWithWorkflowContextType(
                        beanFactory.getBeanDefinition(wbd.beanName()),
                        wbd.beanName(),
                        wbd.workflowContextType())
                )
                .collect(Collectors.groupingBy(BeanDefinitionWithWorkflowContextType::workflowContextType));
    }

    /**
     * Bean definition with workflow context.
     *
     * @param definition          bean definition.
     * @param name                bean name.
     * @param workflowContextType workflow context type.
     */
    record BeanDefinitionWithWorkflowContextType(
            BeanDefinition definition,
            String name,
            Class<? extends WorkflowContext> workflowContextType
    ) {

    }


    @Override
    public void setApplicationContext(@Nonnull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
