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
package io.axoniq.workflow.configuration;

import io.axoniq.workflow.history.inmemory.WorkflowHistoryProjector;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurationDefaults;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;

import static io.axoniq.workflow.configuration.AllEventEventHandlingComponent.ANY_EVENT_IN_ONE_SEGMENT;

/**
 * Enhancer for registration of the workflow engine event processing.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@RegistrationScope(scope = RegistrationScope.Scope.CURRENT)
@Internal
public class WorkflowEventProcessingRegistrationEnhancer implements ConfigurationEnhancer {


    /**
     * Name of the top-level workflow module.
     */
    public static final String DEFAULT_MODULE_NAME = "Workflow";

    private final String engineComponentName;
    private final String projectorComponentName;
    private final String moduleName;
    private final boolean registerHistoryProjector;

    /**
     * Creates a workflow event processing registration enhancer, responsible for registering the workflow engine and
     * history projector components to the event processing module.
     *
     * @param moduleName               name of the event processing module.
     * @param engineComponentName      name of the workflow engine component.
     * @param projectorComponentName   name of the workflow history projector component.
     * @param registerHistoryProjector flag indicating whether to register the history projector component.
     */
    public WorkflowEventProcessingRegistrationEnhancer(
            @Nonnull String moduleName,
            @Nullable String engineComponentName,
            @Nullable String projectorComponentName,
            boolean registerHistoryProjector) {
        this.moduleName = moduleName;
        this.engineComponentName = engineComponentName;
        this.projectorComponentName = projectorComponentName;
        this.registerHistoryProjector = registerHistoryProjector;
    }

    /**
     * Order for this enhancer.
     * <p>
     * Enhancer math: we have to run AFTER the event souring part is set up and let some space for others to register.
     * </p>
     */
    public static final int WORKFLOW_EVENTING_ENHANCER_ORDER = EventSourcingConfigurationDefaults.ENHANCER_ORDER + 20;

    @Override
    public void enhance(@Nonnull ComponentRegistry componentRegistry) {
        componentRegistry.registerModule(
                EventProcessorModule
                        .pooledStreaming(moduleName)
                        .eventHandlingComponents(
                                req -> {
                                    var engineRegistration = req
                                            .declarative(
                                                    engineComponentName != null
                                                            ? engineComponentName + "ExecutionEventing"
                                                            : DEFAULT_MODULE_NAME + "ExecutionEventing",
                                                    cfg -> {
                                                        if (engineComponentName != null) {
                                                            return new AllEventEventHandlingComponent(
                                                                    cfg.getComponent(WorkflowEngine.class,
                                                                                     engineComponentName)
                                                            );
                                                        } else {
                                                            return new AllEventEventHandlingComponent(
                                                                    cfg.getComponent(WorkflowEngine.class)
                                                            );
                                                        }
                                                    }
                                            );
                                    if (registerHistoryProjector) {
                                        engineRegistration = engineRegistration.
                                                declarative(
                                                        projectorComponentName != null ?
                                                                projectorComponentName + "Eventing"
                                                                : DEFAULT_MODULE_NAME + "HistoryEventing",
                                                        cfg -> {
                                                            if (projectorComponentName != null) {
                                                                return new AllEventEventHandlingComponent(
                                                                        cfg.getComponent(
                                                                                WorkflowHistoryProjector.class,
                                                                                projectorComponentName
                                                                        )
                                                                );
                                                            } else {
                                                                return new AllEventEventHandlingComponent(
                                                                        cfg.getComponent(WorkflowHistoryProjector.class)
                                                                );
                                                            }
                                                        }
                                                );
                                    }
                                    return engineRegistration;
                                }
                        )
                        .customized(ANY_EVENT_IN_ONE_SEGMENT)
                        .build()
        );
    }

    @Override
    public int order() {
        return WORKFLOW_EVENTING_ENHANCER_ORDER;
    }
}
