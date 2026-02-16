package io.axoniq.workflow.runtime.test;

import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowDefinitionRegistry;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.execution.ContextToStateAdoptingStateFactory;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.infra.FilesystemStyleComponentDescriptor;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public abstract class AbstractTestBase<T extends WorkflowContext> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final Class<T> dslType;
    private final ComponentBuilder<WorkflowContextFactory<T>> builder;
    protected AxonConfiguration configuration;
    protected WorkflowEngine workflowEngine;
    protected DelayedPublisher delayedPublisher;
    protected WorkflowDefinitionRegistry<?> workflowRegistry;

    public AbstractTestBase(@Nonnull Class<T> dslType, @Nonnull ComponentBuilder<WorkflowContextFactory<T>> builder) {
        this.dslType = dslType;
        this.builder = builder;
    }

    @BeforeEach
    void setUp() {

        var configurer = MessagingConfigurer.create();

        configurer.componentRegistry(r -> r.registerEnhancer(registry ->
                                                                     registry.registerModule(
                                                                             WorkflowModule
                                                                                     .declarative(dslType)
                                                                                     .workflowContextFactory(builder)
                                                                                     .workflowStateFactory(c -> new ContextToStateAdoptingStateFactory<>(
                                                                                             dslType))
                                                                                     .definitions(
                                                                                             getDefinitions()
                                                                                     )
                                                                     )
                                     )
        );

        configuration = configurer.start();
        workflowEngine = configuration.getComponent(WorkflowEngine.class);
        workflowRegistry = configuration.getComponent(WorkflowDefinitionRegistry.class);
        delayedPublisher = configuration.getComponent(DelayedPublisher.class);
    }

    protected abstract Consumer<WorkflowModule.WorkflowDefinitionPhase.DefinitionPhase<T>> getDefinitions();

    @AfterEach
    void shutdown() {
        var descriptor = new FilesystemStyleComponentDescriptor();
        configuration.getComponent(EventSink.class).describeTo(descriptor);
        workflowRegistry.describeTo(descriptor);
        logger.info(descriptor.describe());
        workflowEngine.shutdown();
        configuration.shutdown();
    }
}
