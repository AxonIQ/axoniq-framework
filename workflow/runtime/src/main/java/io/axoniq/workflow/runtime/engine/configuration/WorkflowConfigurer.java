package io.axoniq.workflow.runtime.engine.configuration;

import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurationDefaults;
import org.axonframework.messaging.core.configuration.MessagingConfigurationDefaults;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.configuration.EventBusConfigurationDefaults;

import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.engine.configuration.WorkflowConfigurerDefaults.*;
import static java.util.Objects.requireNonNull;

public class WorkflowConfigurer implements ApplicationConfigurer {

    private final MessagingConfigurer delegate;

    private WorkflowConfigurer(MessagingConfigurer delegate) {
        this.delegate = requireNonNull(delegate, "The Messaging Configurer cannot be null.");
    }

    public static WorkflowConfigurer enhance(MessagingConfigurer messagingConfigurer) {
        return new WorkflowConfigurer(messagingConfigurer)
                .componentRegistry(cr -> cr
                        .registerEnhancer(new EventBusConfigurationDefaults())
                        .registerEnhancer(new MessagingConfigurationDefaults())
                        .registerEnhancer(new EventSourcingConfigurationDefaults())
                        .registerEnhancer(new WorkflowConfigurerDefaults())
                        .registerEnhancer(new WorkflowEventProcessingRegistrationEnhancer(
                                "Workflow",
                                COMPONENT_WORKFLOW_ENGINE,
                                COMPONENT_WORKFLOW_HISTORY_PROJECTOR
                        ))
                );
    }

    public WorkflowConfigurer workflowModule(@Nonnull WorkflowModule<?> workflowModule) {
        return componentRegistry(cr -> cr.registerModule(workflowModule));
    }

    public static WorkflowConfigurer create() {
        return enhance(MessagingConfigurer.create());
    }

    @Override
    @Nonnull
    public WorkflowConfigurer componentRegistry(@Nonnull Consumer<ComponentRegistry> componentRegistrar) {
        delegate.componentRegistry(requireNonNull(componentRegistrar, "The configure task must no be null."));
        return this;
    }

    @Override
    @Nonnull
    public WorkflowConfigurer lifecycleRegistry(@Nonnull Consumer<LifecycleRegistry> lifecycleRegistrar) {
        delegate.lifecycleRegistry(requireNonNull(lifecycleRegistrar, "The lifecycle registrar must not be null."));
        return this;
    }

    @Override
    @Nonnull
    public AxonConfiguration start() {
        return delegate.start();
    }

    @Override
    @Nonnull
    public AxonConfiguration build() {
        return delegate.build();
    }
}
