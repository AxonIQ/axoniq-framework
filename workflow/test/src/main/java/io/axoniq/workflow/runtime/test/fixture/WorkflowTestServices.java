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
package io.axoniq.workflow.runtime.test.fixture;

import io.axoniq.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher;
import io.axoniq.workflow.runtime.test.utils.ManualExecuteStepActionResolver;
import io.axoniq.workflow.runtime.test.utils.ManualWorkflowScheduler;
import io.axoniq.workflow.runtime.test.utils.TestClock;
import io.axoniq.workflow.runtime.test.utils.TestEventPublisher;
import org.jspecify.annotations.Nullable;
import org.axonframework.common.configuration.AxonConfiguration;

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
 * @since 0.2.0
 */
public class WorkflowTestServices {

    private final AxonConfiguration configuration;
    private final WorkflowEngine workflowEngine;
    private final DelayedPublisher delayedPublisher;
    private final TestEventPublisher eventPublisher;
    private final WorkflowConfigurationRegistry<?> workflowRegistry;
    private final WorkflowHistoryRepository workflowHistoryRepository;
    @Nullable
    private final TestClock clock;
    @Nullable
    private final ManualExecuteStepActionResolver executeStepActionResolver;
    @Nullable
    private final ManualWorkflowScheduler timeoutScheduler;

    /**
     * Constructs a new {@link WorkflowTestServices} from the given {@link AxonConfiguration}.
     *
     * @param configuration configuration to use
     * @return initialized services
     */
    public static WorkflowTestServices from(AxonConfiguration configuration) {
        return new WorkflowTestServices(Objects.requireNonNull(configuration, "Configuration must not be null"));
    }

    private WorkflowTestServices(AxonConfiguration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "Configuration must not be null");
        this.workflowEngine = configuration.getComponent(WorkflowEngine.class);
        this.delayedPublisher = configuration.getComponent(DelayedPublisher.class);
        this.eventPublisher = configuration.getComponent(TestEventPublisher.class);
        this.workflowRegistry = configuration.getComponent(WorkflowConfigurationRegistry.class);
        this.workflowHistoryRepository = configuration.getComponent(MutableWorkflowHistoryRepository.class);
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
        ((MutableWorkflowHistoryRepository) workflowHistoryRepository).clear();
    }
}
