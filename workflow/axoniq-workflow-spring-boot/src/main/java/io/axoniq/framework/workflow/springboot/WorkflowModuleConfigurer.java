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

import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration enhancer responsible for creation of {@link WorkflowModule} instances, based on workflow definitions
 * detected by the {@link WorkflowDefinitionLookup}.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
@RegistrationScope("Don't copy this enhancer in order to avoid cyclic module build in Spring Boot.")
public class WorkflowModuleConfigurer implements ConfigurationEnhancer, ApplicationContextAware {

    private final Map<Class<? extends WorkflowContext>, List<String>> workflowDefinitionBeanRefs;
    private final Map<Class<? extends WorkflowContext>, String> workflowContextFactoryBeanRefs;
    private ApplicationContext applicationContext;


    /**
     * Creates a new configurer responsible for registration of found workflows definitions using in a single module.
     */
    @Internal
    WorkflowModuleConfigurer(
            Map<Class<? extends WorkflowContext>, String> workflowContextFactoryBeanRefs,
            Map<Class<? extends WorkflowContext>, List<String>> workflowDefinitionBeanRefs
    ) {
        this.workflowDefinitionBeanRefs = workflowDefinitionBeanRefs;
        this.workflowContextFactoryBeanRefs = workflowContextFactoryBeanRefs;
    }

    @Override
    public void enhance(ComponentRegistry registry) {
        Objects.requireNonNull(applicationContext, "ApplicationContext must not be null");
        workflowDefinitionBeanRefs
                .forEach((workflowContextType, workflowBeanNames) -> register(registry,
                                                                              workflowContextType,
                                                                              workflowBeanNames));
    }

    @SuppressWarnings("unchecked")
    private <C extends WorkflowContext> void register(
            ComponentRegistry registry,
            Class<C> workflowContextType,
            List<String> workflowBeanNames) {
        var factoryName = workflowContextFactoryBeanRefs.get(workflowContextType);
        if (factoryName == null) {
            throw new BadWorkflowConfigurationException(String.format(
                    "Detected workflow definition in '%s' without a WorkflowContextFactory for the workflow type %s.",
                    String.join(",", workflowBeanNames.stream().toList()),
                    workflowContextType.getSimpleName()
            ));
        }
        var workflowContextFactory = (WorkflowContextFactory<C>) applicationContext.getBean(factoryName);
        // All @Workflow beans of the same context type share one module (engine + registry + repository)
        // so version siblings can see each other for multi-version routing.
        var moduleName = workflowContextType.getSimpleName();
        if (workflowBeanNames.isEmpty()) {
            return;
        }
        var withFactory = WorkflowModule
                .defaults(moduleName, workflowContextType)
                .workflowContextFactory(c -> workflowContextFactory);
        var firstBean = workflowBeanNames.get(0);
        ComponentBuilder<Object> firstBuilder = c -> applicationContext.getBean(firstBean);
        WorkflowModule<C> module = withFactory.definition(d -> d.autodetected(firstBuilder));
        for (var beanName : workflowBeanNames.subList(1, workflowBeanNames.size())) {
            ComponentBuilder<Object> builder = c -> applicationContext.getBean(beanName);
            module = module.definition(d -> d.autodetected(builder));
        }
        registry.registerModule(module);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
