/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * Stores which workflow instances are running and which events they wait for, as rows a segment can select on its own.
 * <p>
 * On a claim, the {@link WorkflowEngine} reads the rows of the segment and then the events after their position,
 * instead of sourcing every segment from the {@link WorkflowStore}. The position is kept apart from the token, as a
 * node without the index moves the token but writes no rows. A segment without a position is sourced.
 * <p>
 * One optional index serves every workflow module. Register it once; each module hands it to its engine:
 * <pre>{@code
 * WorkflowConfigurer.create()
 *                   .registerWorkflowIndex(config -> new JpaWorkflowIndex(
 *                           config.getComponent(JpaTransactionalExecutorProvider.class),
 *                           config.getComponent(Converter.class),
 *                           JpaWorkflowIndexConfiguration.DEFAULT
 *                   ));
 * }</pre>
 * <p>
 * Internal, because it is a storage detail of the {@link WorkflowEngine}.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public interface WorkflowIndex {

    /**
     * Loads the rows the given segment owns in the given module, and the position stored for exactly that segment.
     *
     * @param module  the name of the workflow module
     * @param segment the segment to load the rows of
     * @param context the context to read in, or {@code null} to read in a transaction of its own
     * @return a future with the running workflows, the open waits and the position of the segment
     */
    CompletableFuture<WorkflowIndexRows> load(String module, Segment segment, @Nullable ProcessingContext context);

    /**
     * Stores the given changes in their order, then their {@link WorkflowIndexChanges#position() position}. That
     * position replaces the position of every segment whose range overlaps it, as after a split or a merge.
     *
     * @param module  the name of the workflow module
     * @param changes the changes to store
     * @param context the context whose transaction the changes join, or {@code null} for a transaction of their own
     * @return a future completing once the changes are stored
     */
    CompletableFuture<Void> store(String module, WorkflowIndexChanges changes, @Nullable ProcessingContext context);

    /**
     * Returns whether a {@link #store(String, WorkflowIndexChanges, ProcessingContext) store} joins the transaction of
     * the token write, so the owner check of the token also guards the rows. Only then does the engine store that a
     * hibernated workflow woke, as that comes from one node, not from an event.
     *
     * @return {@code true} when rows and token are committed in one transaction
     */
    boolean storesWithToken();
}
