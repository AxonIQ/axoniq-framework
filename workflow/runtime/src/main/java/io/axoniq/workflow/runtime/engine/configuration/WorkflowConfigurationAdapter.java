package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public record WorkflowConfigurationAdapter<C extends WorkflowContext>(
        // DSL level
        ComponentBuilder<WorkflowContextFactory<C>> contextFactoryBuilder,
        ComponentBuilder<WorkflowStateFactory> stateFactoryBuilder,
        // workflow level
        String workflowName,
        ComponentBuilder<EventCondition> startConditionBuilder,
        ComponentBuilder<WorkflowDefinition<C>> definitionBuilder,
        BiFunction<Configuration, WorkflowCustomization, WorkflowCustomization> instanceCustomization
) implements ComponentBuilder<ConditionedWorkflowConfiguration<?>> {


    @Override
    public ConditionedWorkflowConfiguration<C> build(@Nonnull Configuration configuration) {
        var workflowModuleConfiguration = instanceCustomization.apply(configuration,
                                                                      WorkflowCustomization.defaultConfiguration(
                                                                              workflowName,
                                                                              configuration));

        return new ConditionedWorkflowConfiguration<>(
                startConditionBuilder.build(configuration),
                new WorkflowConfiguration<>() {

                    @Nonnull
                    @Override
                    public String workflowName() {
                        return workflowName;
                    }

                    @NotNull
                    @Override
                    public WorkflowDefinition<C> workflowDefinition() {
                        return definitionBuilder.build(configuration);
                    }

                    @NotNull
                    @Override
                    public WorkflowContextFactory<C> workflowContextFactory() {
                        return contextFactoryBuilder.build(configuration);
                    }

                    @NotNull
                    @Override
                    public WorkflowStateFactory workflowStateFactory() {
                        return stateFactoryBuilder.build(configuration);
                    }

                    @NotNull
                    @Override
                    public WorkflowIdProvider workflowIdProvider() {
                        return workflowModuleConfiguration.workflowIdProvider;
                    }

                    @NotNull
                    @Override
                    public EventNameCustomizer eventNameCustomizer() {
                        return workflowModuleConfiguration.eventNameCustomizer;
                    }

                    @Nonnull
                    @Override
                    public Map<WorkflowStatus, WorkflowStatusChangeListener> workflowStatusChangeListeners() {
                        var result = new HashMap<WorkflowStatus, WorkflowStatusChangeListener>();
                        workflowModuleConfiguration.workflowStatusListeners.forEach((k, v) -> {
                            if (!v.isEmpty()) {
                                result.put(k, v);
                            }
                        });
                        return Collections.unmodifiableMap(result);
                    }
                }
        );
    }
}
