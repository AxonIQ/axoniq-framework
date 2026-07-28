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
import io.axoniq.workflow.runtime.execution.SafePointStore;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurationDefaults;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;

import java.util.concurrent.CompletableFuture;

import static io.axoniq.workflow.configuration.AllEventEventHandlingComponent.ANY_EVENT_IN_ONE_SEGMENT;
import static io.axoniq.workflow.configuration.WorkflowConfigurationDefaults.COMPONENT_SAFE_POINT_STORE;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Enhancer for registration of the workflow engine event processing.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@RegistrationScope(scope = RegistrationScope.Scope.CURRENT)
@Internal
public class WorkflowEventProcessingRegistrationEnhancer implements ConfigurationEnhancer {

    private static final int PRE_PROCESSOR_START_PHASE = Phase.INBOUND_EVENT_CONNECTORS - 10;

    /**
     * Name of the top-level workflow module.
     */
    public static final String DEFAULT_MODULE_NAME = "Workflow";

    private final String moduleName;
    @Nullable
    private final String engineComponentName;
    @Nullable
    private final String projectorComponentName;
    private final boolean registerHistoryProjector;

    /**
     * Creates a workflow event processing registration enhancer, responsible for registering the workflow engine and
     * history projector components to the event processing module.
     *
     * @param moduleName               name of the event processing module
     * @param engineComponentName      name of the workflow engine component
     * @param projectorComponentName   name of the workflow history projector component
     * @param registerHistoryProjector flag indicating whether to register the history projector component
     */
    public WorkflowEventProcessingRegistrationEnhancer(
            String moduleName,
            @Nullable String engineComponentName,
            @Nullable String projectorComponentName,
            boolean registerHistoryProjector
    ) {
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
    public void enhance(ComponentRegistry componentRegistry) {
        componentRegistry.registerModule(
                EventProcessorModule
                        .pooledStreaming(moduleName)
                        .eventHandlingComponents(
                                req -> {
                                    var cp = req.declarative(
                                            engineComponentName != null
                                                    ? engineComponentName + "ExecutionEventing"
                                                    : DEFAULT_MODULE_NAME + "ExecutionEventing",
                                            cfg -> {
                                                if (engineComponentName != null) {
                                                    return new AllEventEventHandlingComponent(
                                                            cfg.getComponent(WorkflowEngine.class,
                                                                             engineComponentName));
                                                } else {
                                                    return new AllEventEventHandlingComponent(
                                                            cfg.getComponent(WorkflowEngine.class));
                                                }
                                            }
                                    );
                                    if (registerHistoryProjector) {
                                        cp = cp.declarative(
                                                projectorComponentName != null
                                                        ? projectorComponentName + "Eventing"
                                                        : DEFAULT_MODULE_NAME + "HistoryEventing",
                                                cfg -> {
                                                    if (projectorComponentName != null) {
                                                        return new AllEventEventHandlingComponent(
                                                                cfg.getComponent(WorkflowHistoryProjector.class,
                                                                                 projectorComponentName)
                                                        );
                                                    } else {
                                                        // FIXME: eventually history projector doesn't need to be replayed.
                                                        // configure this separately InMemoryHistoryRepo = InMemoryTokeStore and replay
                                                        return new AllEventEventHandlingComponent(
                                                                cfg.getComponent(WorkflowHistoryProjector.class)
                                                        );
                                                    }
                                                }
                                        );
                                    }
                                    return cp;
                                }
                        )
                        .customized(ANY_EVENT_IN_ONE_SEGMENT)
                        .componentRegistry(cr -> cr.registerComponent(
                                ComponentDefinition
                                        .ofTypeAndName(Object.class, moduleName + "ReplayResetHook")
                                        .withInstance(new Object())
                                        .onStart(PRE_PROCESSOR_START_PHASE, (cfg, ignored) -> {
                                            // @formatter:off
                                                     var tokenStore = cfg.getComponent(TokenStore.class, tokenStoreComponentName(moduleName));
                                                     var eventSource = cfg.getComponent(StreamableEventSource.class);
                                                     var processor = cfg.getComponent(StreamingEventProcessor.class, moduleName);
                                                     var unitOfWorkFactory = cfg.getComponent(UnitOfWorkFactory.class);
                                                     var safePointStore = cfg.getComponent(SafePointStore.class, COMPONENT_SAFE_POINT_STORE);
                                                     var workflowEngine = engineComponentName != null
                                                             ? cfg.getComponent(WorkflowEngine.class, engineComponentName)
                                                             : cfg.getComponent(WorkflowEngine.class);
                                                     return ensureSegmentsInitialized(tokenStore, eventSource)
                                                             .thenCompose(latestToken -> resetOrSwitchToLiveMode(
                                                                     eventSource,
                                                                     processor,
                                                                     workflowEngine,
                                                                     safePointStore,
                                                                     unitOfWorkFactory,
                                                                     latestToken
                                                             ));
                                            // @formatter:off
                                                 }
                                        )
                        ))
                        .build()
        );
    }

    private CompletableFuture<TrackingToken> ensureSegmentsInitialized(
            TokenStore tokenStore,
            StreamableEventSource eventSource
    ) {
        return eventSource
                .latestToken(null)
                .thenCompose(latestToken -> tokenStore
                        .fetchSegments(moduleName, null)
                        .thenCompose(segments -> (!segments.isEmpty())
                                                                        ? completedFuture(latestToken)
                                                                        : tokenStore.initializeTokenSegments(
                                                                                             moduleName,
                                                                                             1, // FIXME #190 (https://github.com/AxonIQ/extension-workflow/issues/190) -> should be configurable?
                                                                                             latestToken,
                                                                                             null
                                                                                     )
                                                                                     .thenApply(ignored -> latestToken)));
    }

    CompletableFuture<Void> resetOrSwitchToLiveMode(
            StreamableEventSource eventSource,
            StreamingEventProcessor processor,
            WorkflowEngine workflowEngine,
            SafePointStore safePointStore,
            UnitOfWorkFactory unitOfWorkFactory,
            @Nullable TrackingToken latestToken
    ) {
        return determineResetToken(eventSource, safePointStore)
                .thenCompose(resetToken -> unitOfWorkFactory.create(moduleName + "WorkflowRehydration")
                                        .executeWithResult(context -> {
                                            workflowEngine.start(resetToken, context);
                                            return completedFuture(null);
                                        })
                                        .thenCompose(ignored -> switchToLiveOrReplay(
                                                processor,
                                                workflowEngine,
                                                resetToken,
                                                latestToken
                                        )));
    }

    private CompletableFuture<Void> switchToLiveOrReplay(StreamingEventProcessor processor,
                                                         WorkflowEngine workflowEngine,
                                                         @Nullable TrackingToken resetToken,
                                                         @Nullable TrackingToken latestToken) {
        if (!requiresReplay(resetToken, latestToken)) {
            workflowEngine.switchToLiveMode();
            return completedFuture(null);
        }
        return processor.resetTokens(resetToken);
    }

    private CompletableFuture<TrackingToken> determineResetToken(
            StreamableEventSource eventSource,
            SafePointStore safePointStore
    ) {
        return safePointStore.fetchSafePointToken()
                             .thenCompose(storedToken -> {
                                 if (storedToken != null) {
                                     return completedFuture(storedToken);
                                 }
                                 return eventSource.firstToken(null);
                             });
    }

    boolean requiresReplay(@Nullable TrackingToken resetToken, @Nullable TrackingToken latestToken) {
        if (resetToken == null || latestToken == null) {
            return false;
        }
        return !resetToken.samePositionAs(latestToken);
    }

    @Override
    public int order() {
        return WORKFLOW_EVENTING_ENHANCER_ORDER;
    }

    /**
     * Returns the name of the token store component for the given module used for workflow event processing.
     *
     * @param moduleName module name
     * @return name of the token store component
     */
    public static String tokenStoreComponentName(String moduleName) {
        return "TokenStore[" + moduleName + "]";
    }
}
