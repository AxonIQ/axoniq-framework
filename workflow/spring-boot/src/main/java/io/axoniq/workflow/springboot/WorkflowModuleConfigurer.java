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
package io.axoniq.workflow.springboot;

import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.execution.DSLAdoptingExecutionFactory;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
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
    private @Nullable ApplicationContext applicationContext;


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
        workflowDefinitionBeanRefs.forEach((workflowContextType, workflowBeanNames) -> {

            var factoryName = workflowContextFactoryBeanRefs.get(workflowContextType);
            if (factoryName != null) {
                @SuppressWarnings("unchecked")
                var dslPhase = WorkflowModule
                        .usingContext((Class<WorkflowContext>) workflowContextType)
                        .workflowContextFactory(
                                c -> (WorkflowContextFactory<WorkflowContext>) applicationContext.getBean(factoryName))
                        .workflowExecutionFactory(
                                c -> new DSLAdoptingExecutionFactory<>(workflowContextType));

                if (!workflowBeanNames.isEmpty()) {
                    WorkflowModule<?> module = null;
                    for (var beanName : workflowBeanNames) {
                        //noinspection unchecked
                        module = dslPhase
                                .definitions(d -> d.autodetected(
                                                     c -> applicationContext.getBean(beanName),
                                                     ((Class<WorkflowContext>) workflowContextType)
                                             )
                                );
                    }
                    registry.registerModule(module); // FIXME: move towards one module per workflow definition
                }
            } else {
                throw new BadWorkflowConfigurationException(String.format(
                        "Detected workflow definition in '%s' without a WorkflowContextFactory for the workflow type %s.",
                        String.join(",", workflowBeanNames.stream().toList()),
                        workflowContextType.getSimpleName()
                ));
            }
        });
    }

    @Override
    public void setApplicationContext(@Nonnull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
