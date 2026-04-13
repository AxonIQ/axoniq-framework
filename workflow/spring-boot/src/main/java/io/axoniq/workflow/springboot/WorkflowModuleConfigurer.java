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

import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.jspecify.annotations.NonNull;
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
 * @since 1.0.0
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
    public void enhance(@NonNull ComponentRegistry registry) {
        Objects.requireNonNull(applicationContext, "ApplicationContext must not be null");
        workflowDefinitionBeanRefs
                .forEach((workflowContextType, workflowBeanNames) -> register(registry,
                                                                              workflowContextType,
                                                                              workflowBeanNames));
    }

    @SuppressWarnings("unchecked")
    private <C extends WorkflowContext> void register(
            @Nonnull ComponentRegistry registry,
            @Nonnull Class<C> workflowContextType,
            @Nonnull List<String> workflowBeanNames) {
        var factoryName = workflowContextFactoryBeanRefs.get(workflowContextType);
        if (factoryName != null) {
            var workflowContextFactory = (WorkflowContextFactory<C>) applicationContext.getBean(factoryName);
            for (var beanName : workflowBeanNames) {
                ComponentBuilder<Object> builder = c -> applicationContext.getBean(beanName);
                registry.registerModule(
                        WorkflowModule
                                .defaults(beanName, workflowContextType)
                                .workflowContextFactory(c -> workflowContextFactory)
                                .definition(d -> d
                                        .autodetected(builder)
                                )
                );
            }
        } else {
            throw new BadWorkflowConfigurationException(String.format(
                    "Detected workflow definition in '%s' without a WorkflowContextFactory for the workflow type %s.",
                    String.join(",", workflowBeanNames.stream().toList()),
                    workflowContextType.getSimpleName()
            ));
        }
    }

    @Override
    public void setApplicationContext(@Nonnull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
