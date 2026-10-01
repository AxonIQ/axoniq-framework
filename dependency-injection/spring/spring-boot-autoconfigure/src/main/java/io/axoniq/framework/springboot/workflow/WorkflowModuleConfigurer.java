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
package io.axoniq.framework.springboot.workflow;

import io.axoniq.framework.springboot.WorkflowProperties;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import org.axonframework.common.AxonThreadFactory;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

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
    private ApplicationContext applicationContext;


    /**
     * Creates a new configurer responsible for registration of found workflows definitions using in a single module.
     */
    @Internal
    WorkflowModuleConfigurer(Map<Class<? extends WorkflowContext>, List<String>> workflowDefinitionBeanRefs) {
        this.workflowDefinitionBeanRefs = workflowDefinitionBeanRefs;
    }

    @Override
    public void enhance(ComponentRegistry registry) {
        if (applicationContext == null) {
            throw new IllegalStateException("ApplicationContext must not be null");
        }
        var workflowProperties = applicationContext.getBean(WorkflowProperties.class);
        workflowDefinitionBeanRefs.forEach((workflowContextType, workflowBeanNames) -> register(
                registry, workflowContextType, workflowBeanNames, workflowProperties
        ));
    }

    @SuppressWarnings("unchecked")
    private <C extends WorkflowContext> void register(
            ComponentRegistry registry,
            Class<C> workflowContextType,
            List<String> workflowBeanNames,
            WorkflowProperties workflowProperties
    ) {
        // All @Workflow beans of the same context type share one module (engine + registry + repository)
        // so version siblings can see each other for multi-version routing.
        var moduleName = workflowContextType.getSimpleName();
        if (workflowBeanNames.isEmpty()) {
            return;
        }
        var afterHistory = WorkflowModule.configure(moduleName, workflowContextType)
                                         .historyProcessorConfiguration(processorConfiguration -> applyHistoryProperties(
                                                 workflowProperties, processorConfiguration
                                         ));
        var firstBean = workflowBeanNames.getFirst();
        ComponentBuilder<Object> firstBuilder = c -> applicationContext.getBean(firstBean);
        WorkflowModule.OptionalPhase<C> module =
                afterHistory.definition(d -> d.autodetected(firstBuilder))
                            .processorConfiguration(psepConfig -> applyProperties(workflowProperties, psepConfig))
                            .contextFactory(configuration -> (WorkflowContextFactory<C>) configuration
                                    .getOptionalComponent(WorkflowContextFactory.class, workflowContextType.getName())
                                    .orElseThrow(() -> new BadWorkflowConfigurationException(String.format(
                                            "Detected workflow definition in '%s' without a WorkflowContextFactory for the workflow type %s.",
                                            String.join(",", workflowBeanNames),
                                            workflowContextType.getSimpleName()
                                    ))));
        for (var beanName : workflowBeanNames.subList(1, workflowBeanNames.size())) {
            ComponentBuilder<Object> builder = c -> applicationContext.getBean(beanName);
            module = module.definition(d -> d.autodetected(builder));
        }
        registry.registerModule(module);
    }

    static PooledStreamingEventProcessorConfiguration applyProperties(
            WorkflowProperties workflowProperties,
            PooledStreamingEventProcessorConfiguration processorConfiguration
    ) {
        return applyProcessorProperties(
                "workflowEngine",
                workflowProperties.getInitialSegmentCount(),
                workflowProperties.getBatchSize(),
                workflowProperties.getThreadCount(),
                workflowProperties.getTokenClaimInterval(),
                workflowProperties.getClaimExtensionThreshold(),
                workflowProperties.getCoordinatorClaimExtension(),
                processorConfiguration
        );
    }

    static PooledStreamingEventProcessorConfiguration applyHistoryProperties(
            WorkflowProperties workflowProperties,
            PooledStreamingEventProcessorConfiguration processorConfiguration
    ) {
        WorkflowProperties.HistoryProcessorProperties history = workflowProperties.getHistory();
        return applyProcessorProperties(
                "workflowHistoryProjector",
                history.getInitialSegmentCount(),
                history.getBatchSize(),
                history.getThreadCount(),
                history.getTokenClaimInterval(),
                history.getClaimExtensionThreshold(),
                history.getCoordinatorClaimExtension(),
                processorConfiguration
        );
    }

    private static PooledStreamingEventProcessorConfiguration applyProcessorProperties(
            String name,
            int initialSegmentCount,
            int batchSize,
            int threadCount,
            long tokenClaimInterval,
            long claimExtensionThreshold,
            boolean coordinatorClaimExtension,
            PooledStreamingEventProcessorConfiguration processorConfiguration
    ) {
        String executorName = "WorkPackage[" + name + "]";
        Supplier<ScheduledExecutorService> scheduledExecutorService = () -> Executors.newScheduledThreadPool(
                threadCount,
                new AxonThreadFactory(executorName)
        );
        processorConfiguration.workerExecutor(scheduledExecutorService)
                              .tokenClaimInterval(tokenClaimInterval)
                              .claimExtensionThreshold(claimExtensionThreshold)
                              .batchSize(batchSize)
                              .initialSegmentCount(initialSegmentCount);
        if (coordinatorClaimExtension) {
            processorConfiguration.enableCoordinatorClaimExtension();
        }
        return processorConfiguration;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
