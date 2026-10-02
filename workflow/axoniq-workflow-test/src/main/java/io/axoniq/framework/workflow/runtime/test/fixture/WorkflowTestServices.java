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
package io.axoniq.framework.workflow.runtime.test.fixture;

import io.axoniq.framework.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.runtime.execution.WorkflowCancellationService;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher;
import io.axoniq.framework.workflow.runtime.test.utils.ManualExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.test.utils.ManualWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.test.utils.TestClock;
import io.axoniq.framework.workflow.runtime.test.utils.TestEventPublisher;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentBuilder;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * Aggregates workflow runtime services used by workflow tests.
 *
 * <p>This type is a small convenience wrapper around {@link AxonConfiguration}. It resolves the runtime services that
 * workflow tests commonly need, such as the {@link WorkflowEngine}, test event publication helpers, workflow
 * configuration registry, and workflow history repository. It also exposes stepping-mode-only services through
 * {@link Optional}, so callers can detect whether the underlying test configuration was started with stepping
 * support.</p>
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class WorkflowTestServices {

    /**
     * Name every {@link io.axoniq.framework.workflow.configuration.WorkflowModule} built through
     * {@link WorkflowTestFixture#workflowModule(Class, ComponentBuilder, ComponentBuilder)} is registered under.
     */
    public static final String DEFAULT_MODULE_NAME = "test";

    private final AxonConfiguration configuration;
    private final WorkflowEngine workflowEngine;
    private final DelayedPublisher delayedPublisher;
    private final TestEventPublisher eventPublisher;
    private final WorkflowConfigurationRegistry<?> workflowRegistry;
    private final WorkflowCancellationService workflowCancellationService;
    private final MutableWorkflowHistoryRepository workflowHistoryRepository;
    @Nullable
    private final TestClock clock;
    @Nullable
    private final ManualExecuteStepActionResolver executeStepActionResolver;
    @Nullable
    private final ManualWorkflowScheduler timeoutScheduler;

    /**
     * Constructs a new {@code WorkflowTestServices} from the given {@link AxonConfiguration}.
     *
     * @param configuration configuration to use
     * @return initialized services
     */
    public static WorkflowTestServices from(AxonConfiguration configuration) {
        return from(configuration, DEFAULT_MODULE_NAME);
    }

    /**
     * Constructs a new {@code WorkflowTestServices} from the given {@link AxonConfiguration}, using the given
     * {@code moduleName} as the name for the {@link io.axoniq.framework.workflow.configuration.WorkflowModule} under
     * test.
     *
     * @param configuration configuration to use
     * @param moduleName    name of the {@link io.axoniq.framework.workflow.configuration.WorkflowModule} under test
     * @return initialized services
     */
    public static WorkflowTestServices from(AxonConfiguration configuration, String moduleName) {
        return new WorkflowTestServices(
                Objects.requireNonNull(configuration, "Configuration must not be null"),
                Objects.requireNonNull(moduleName, "Module name must not be null")
        );
    }

    private WorkflowTestServices(AxonConfiguration configuration, String moduleName) {
        this.configuration = configuration;
        String engineName = "WorkflowEngine[" + moduleName + "]";
        this.workflowEngine = Objects.requireNonNull(configuration.getComponents(WorkflowEngine.class).get(engineName),
                                                     "WorkflowEngine with name [" + engineName + "] must not be null.");
        this.delayedPublisher = configuration.getComponent(DelayedPublisher.class);
        this.eventPublisher = configuration.getComponent(TestEventPublisher.class);
        String registryName = "WorkflowConfigurationRegistry[" + moduleName + "]";
        this.workflowRegistry = Objects.requireNonNull(
                configuration.getComponents(WorkflowConfigurationRegistry.class).get(registryName),
                "WorkflowConfigurationRegistry with name [" + registryName + "] must not be null."
        );
        String cancellationServiceName = "WorkflowCancellationService[" + moduleName + "]";
        this.workflowCancellationService = Objects.requireNonNull(
                configuration.getComponents(WorkflowCancellationService.class).get(cancellationServiceName),
                "WorkflowCancellationService with name [" + cancellationServiceName + "] must not be null."
        );
        String historyRepositoryName = "MutableWorkflowHistoryRepository[" + moduleName + "]";
        this.workflowHistoryRepository = Objects.requireNonNull(
                configuration.getComponents(MutableWorkflowHistoryRepository.class).get(historyRepositoryName),
                "MutableWorkflowHistoryRepository with name [" + historyRepositoryName + "] must not be null."
        );
        this.clock = configuration.getOptionalComponent(TestClock.class).orElse(null);
        this.executeStepActionResolver = configuration.getOptionalComponent(ManualExecuteStepActionResolver.class)
                                                      .orElse(null);
        this.timeoutScheduler = configuration.getOptionalComponent(ManualWorkflowScheduler.class)
                                             .orElse(null);
    }

    /**
     * Returns the underlying Axon configuration
     *
     * @return configuration backing these test services
     */
    public AxonConfiguration configuration() {
        return configuration;
    }

    /**
     * Returns the workflow engine used by the test runtime
     *
     * @return workflow engine
     */
    public WorkflowEngine workflowEngine() {
        return workflowEngine;
    }

    /**
     * Returns the delayed event publisher used for integration-style test publication
     *
     * @return delayed publisher
     */
    public DelayedPublisher delayedPublisher() {
        return delayedPublisher;
    }

    /**
     * Returns the test event publisher used to inject events into the workflow runtime
     *
     * @return test event publisher
     */
    public TestEventPublisher eventPublisher() {
        return eventPublisher;
    }

    /**
     * Returns the workflow configuration registry available to the test runtime
     *
     * @return workflow configuration registry
     */
    public WorkflowConfigurationRegistry<?> workflowRegistry() {
        return workflowRegistry;
    }

    /**
     * Returns the workflow cancellation service available to the test runtime
     *
     * @return workflow cancellation service
     */
    public WorkflowCancellationService workflowCancellationService() {
        return workflowCancellationService;
    }

    /**
     * Returns the workflow history repository used by the test runtime
     *
     * @return workflow history repository
     */
    public WorkflowHistoryRepository workflowHistoryRepository() {
        return workflowHistoryRepository;
    }

    /**
     * Returns the stepping-mode test clock when the configuration provides one
     *
     * @return optional mutable test clock
     */
    public Optional<TestClock> clock() {
        return Optional.ofNullable(clock);
    }

    /**
     * Returns the manual execute-step action resolver when the configuration provides one
     *
     * @return optional manual execute-step action resolver
     */
    public Optional<ManualExecuteStepActionResolver> executeStepActionResolver() {
        return Optional.ofNullable(executeStepActionResolver);
    }

    /**
     * Returns the manual workflow scheduler when the configuration provides one
     *
     * @return optional manual workflow scheduler
     */
    public Optional<ManualWorkflowScheduler> timeoutScheduler() {
        return Optional.ofNullable(timeoutScheduler);
    }


    /**
     * Shuts down the workflow test services and clears recorded workflow history
     */
    public void shutdown() {
        workflowEngine.shutdown();
        configuration.shutdown();
        workflowHistoryRepository.clear();
    }
}
