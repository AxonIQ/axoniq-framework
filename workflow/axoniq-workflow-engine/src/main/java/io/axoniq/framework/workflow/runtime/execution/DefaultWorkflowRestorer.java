/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.util.ProcessingContextUtils;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link WorkflowRestorer} of a {@link WorkflowEngine}: sources a workflow from the {@link WorkflowStore} in a unit
 * of work of its own, and creates its execution from the configuration of its definition.
 * <p>
 * <b>Its own unit of work.</b> The position an execution resumes appending from has to be the one its own read ended
 * at. A transaction shared by every workflow a claim restores ends at the lowest of its reads, and a previous owner
 * appending while the claim is still reading leaves that position before an event the claim itself sourced. The
 * restored execution would then be rejected by its own history and stop, even though its state is current. Reading one
 * workflow per transaction leaves each position at or after that workflow's last event, so only a write that lands
 * after this read rejects the execution, which is a foreign writer and exactly what should stop it. The unit of work
 * holds every resource of the given context except its event store transaction, which is what makes the read its own.
 * <p>
 * <b>Its configuration.</b> The execution is created from the configuration registered for the definition its state
 * records, or else from the closest registered version, the way the replay path routes it: a body may have moved its
 * recorded version forward to a version no definition is statically registered under. A workflow whose definition no
 * configuration matches belongs to another workflow module, and fails the restore with an
 * {@link UnrecognizedWorkflowDefinitionException}.
 * <p>
 * The {@link WorkflowEngine} creates one, restores the workflows of the segments it claims with it, and hands it to its
 * {@link WorkflowEngineHibernationSupport}, which wakes the hibernated workflows with it.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
final class DefaultWorkflowRestorer implements WorkflowRestorer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultWorkflowRestorer.class);

    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private final WorkflowStore workflowStore;
    private final UnitOfWorkFactory unitOfWorkFactory;

    /**
     * Creates the restorer of an engine.
     *
     * @param workflowConfigurationRegistry the configurations of the workflow definitions of the engine
     * @param workflowStore                 the store the durable state of a workflow is sourced from
     * @param unitOfWorkFactory             creates the unit of work each workflow is sourced in
     */
    DefaultWorkflowRestorer(WorkflowConfigurationRegistry<?> workflowConfigurationRegistry,
                            WorkflowStore workflowStore,
                            UnitOfWorkFactory unitOfWorkFactory) {
        this.workflowConfigurationRegistry = Objects.requireNonNull(
                workflowConfigurationRegistry, "The WorkflowConfigurationRegistry must not be null."
        );
        this.workflowStore = Objects.requireNonNull(workflowStore, "The WorkflowStore must not be null.");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "The UnitOfWorkFactory must not be null.");
    }

    @Override
    public CompletableFuture<Optional<WorkflowExecution>> restore(String workflowId,
                                                                  ProcessingContext resources,
                                                                  ProcessingContext executionContext) {
        Objects.requireNonNull(workflowId, "The workflow identifier must not be null.");
        Objects.requireNonNull(resources, "The resources context must not be null.");
        Objects.requireNonNull(executionContext, "The execution context must not be null.");
        return unitOfWorkFactory
                .create("restore-" + workflowId)
                .executeWithResult(sourcingContext -> {
                    // Everything the given context reads with, except its event store transaction: that is what makes
                    // this read, and the position it ends at, this workflow's own.
                    ProcessingContextUtils.copyResources(resources, sourcingContext);
                    return workflowStore.loadWorkflow(workflowId, sourcingContext)
                                        .thenApply(state -> new Sourced(state, sourcedAt(sourcingContext)));
                })
                .thenApply(sourced -> executionOf(workflowId, sourced, executionContext));
    }

    /**
     * Returns the position the given context's sourcing ended at.
     * <p>
     * The current Event Store does not add its append position to the processing context when sourcing. Retrieve it
     * from the context-bound transaction until {@link ConsistencyMarker} provides a corresponding context accessor. The
     * missing-component path keeps focused engine tests that construct an engine without a full workflow configuration
     * independent of the Event Store.
     *
     * @param sourcingContext context used to source the workflow state
     * @return the marker at which sourcing ended
     */
    @Nullable
    private static ConsistencyMarker sourcedAt(ProcessingContext sourcingContext) {
        try {
            return sourcingContext.component(EventStore.class).transaction(sourcingContext).appendPosition();
        } catch (ComponentNotFoundException e) {
            return null;
        }
    }

    /**
     * Creates the execution of the given workflow from its sourced state, resuming its appends from the position that
     * read ended at, unless that state is terminal.
     */
    private Optional<WorkflowExecution> executionOf(String workflowId,
                                                    Sourced sourced,
                                                    ProcessingContext executionContext) {
        var state = sourced.state();
        var status = state.workflowStatus();
        if (status.isTerminal()) {
            logger.debug("Not restoring workflow '{}': its state is {}.", workflowId, status);
            return Optional.empty();
        }
        var definitionId = state.workflowDefinitionId();
        var workflowName = definitionId.qualifiedName().toString();
        // Same routing as the replay path: a body may have moved its recorded version forward with
        // ctx.migrateVersion(...) to a version no definition is statically registered under, so an exact lookup alone
        // cannot restore it.
        var workflowConfiguration = workflowConfigurationRegistry
                .getWorkflowConfiguration(definitionId)
                .or(() -> workflowConfigurationRegistry.findClosestRegisteredVersion(
                        workflowName, definitionId.version()
                ))
                .or(() -> workflowConfigurationRegistry.findClosestHigherRegisteredVersion(
                        workflowName, definitionId.version()
                ))
                .orElseThrow(() -> new UnrecognizedWorkflowDefinitionException(
                        "No workflow configuration found for workflow '%s' with definition %s."
                                .formatted(workflowId, definitionId)
                ));
        executionContext.putResource(WorkflowConfigurationRegistry.RESOURCE_KEY, workflowConfigurationRegistry);
        var workflowContext = workflowConfiguration.workflowContextFactory().createContext(
                state.payload(),
                workflowId,
                executionContext,
                workflowConfiguration
        );
        var execution = workflowConfiguration.workflowExecutionFactory().create(workflowContext);
        execution.initializeState(state);
        execution.restoreAppendPosition(sourced.position());
        return Optional.of(execution);
    }

    /**
     * The state of a workflow sourced on its own, and the position that read ended at.
     */
    private record Sourced(WorkflowState state, @Nullable ConsistencyMarker position) {

    }
}
