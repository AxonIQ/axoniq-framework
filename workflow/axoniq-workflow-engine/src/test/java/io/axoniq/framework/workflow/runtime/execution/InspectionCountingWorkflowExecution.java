/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A running {@link WorkflowExecution} that only knows its workflow id, counts every time anyone inspects it, and
 * records the events delivered to it and the checkpoint work state listener registered on it. Any other interaction
 * fails, so a test notices when the code under test does more with it than inspecting it, delivering an event, or
 * listening to its safe points.
 *
 * @author Allard Buijze
 */
final class InspectionCountingWorkflowExecution implements WorkflowExecution {

    private final String workflowId;
    private final AtomicInteger inspections = new AtomicInteger();
    private final List<EventMessage> deliveredEvents = new CopyOnWriteArrayList<>();
    private volatile @Nullable CheckpointWorkStateListener checkpointWorkStateListener;

    InspectionCountingWorkflowExecution(String workflowId) {
        this.workflowId = workflowId;
    }

    /**
     * Returns the workflow id without counting an inspection, for a test to tell which segment owns this execution.
     */
    String id() {
        return workflowId;
    }

    /**
     * Returns how often this execution was inspected: asked for its id or whether it runs, or handed an event.
     */
    int inspections() {
        return inspections.get();
    }

    List<EventMessage> deliveredEvents() {
        return List.copyOf(deliveredEvents);
    }

    /**
     * Returns the checkpoint work state listener last registered on this execution, through which a test reports the
     * safe points of this execution, as a running execution does.
     */
    @Nullable CheckpointWorkStateListener checkpointWorkStateListener() {
        return checkpointWorkStateListener;
    }

    @Override
    public String workflowId() {
        inspections.incrementAndGet();
        return workflowId;
    }

    @Override
    public boolean isRunning() {
        inspections.incrementAndGet();
        return true;
    }

    @Override
    public void onEvent(EventMessage eventMessage, ProcessingContext processingContext) {
        inspections.incrementAndGet();
        deliveredEvents.add(eventMessage);
    }

    @Override
    public CompletableFuture<Void> execute(Consumer<WorkflowExecution> terminationHandler) {
        throw unexpected();
    }

    @Override
    public WorkflowExecutionOperations workflowExecutionOperations() {
        throw unexpected();
    }

    @Override
    public void awaitStateChange(Predicate<WorkflowState> condition) {
        throw unexpected();
    }

    @Override
    public void appendTask(Consumer<WorkflowExecution> task) {
        throw unexpected();
    }

    @Override
    public void interruptWorkflowDriver() {
        throw unexpected();
    }

    @Override
    public CompletableFuture<Void> appendWorkflowEvent(EventMessage event, Context parentContext) {
        throw unexpected();
    }

    @Override
    public @Nullable Consumer<WorkflowExecution> getNextTask() {
        throw unexpected();
    }

    @Override
    public boolean hasTasks() {
        throw unexpected();
    }

    @Override
    public void addCheckpointLatch(Runnable latch) {
        throw unexpected();
    }

    @Override
    public boolean hasUnsafeCheckpointWork() {
        throw unexpected();
    }

    @Override
    public void registerCheckpointWorkStateListener(CheckpointWorkStateListener listener) {
        checkpointWorkStateListener = listener;
    }

    @Override
    public void stopForShutdown() {
        throw unexpected();
    }

    @Override
    public WorkflowState state() {
        throw unexpected();
    }

    @Override
    public void initializeState(WorkflowState state) {
        throw unexpected();
    }

    @Override
    public ProcessingContext processingContext() {
        throw unexpected();
    }

    @Override
    public String workflowName() {
        throw unexpected();
    }

    @Override
    public WorkflowConfiguration<?> workflowConfiguration() {
        throw unexpected();
    }

    @Override
    public void restoreAppendPosition(@Nullable ConsistencyMarker position) {
        throw unexpected();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("workflowId", workflowId);
    }

    private UnsupportedOperationException unexpected() {
        return new UnsupportedOperationException("Not expected to be called on workflow execution " + workflowId);
    }
}
