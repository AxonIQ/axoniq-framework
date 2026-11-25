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
 * Without an index, the {@link WorkflowEngine} learns both from the {@link WorkflowStore} each time its processor
 * claims a segment: it sources the {@link RunningWorkflows} and the {@link WaitAssociations} of every segment, and
 * keeps what the claimed segment owns. With an index, the engine reads only the rows in the
 * {@link SegmentKeyRange} of the claimed segment, and then the events after the token the segment resumes from.
 * <p>
 * The index has no position of its own. The engine writes the rows that changed each time the processor stores its
 * token, through
 * {@link io.axoniq.framework.messaging.eventstreaming.checkpoint.Checkpointing#onCheckpointStore(Segment,
 * org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken, ProcessingContext)}. An
 * implementation that {@link #storesWithToken() stores with the token} writes them in the transaction of that token, so
 * the rows and the token are committed together and the token is the position of the rows.
 * <p>
 * The rows are derived from events, which stay the source of truth. Every {@link WorkflowIndexChange} has the same
 * outcome when it is stored twice, so rows that are ahead of the token are corrected by the events a claim reads.
 * <p>
 * The index is optional and one component serves every workflow module, as each row carries the name of its module.
 * Register it through {@code WorkflowConfigurer.registerWorkflowIndex(...)}; the workflow module hands it to its
 * engine. Do not call it directly.
 * <p>
 * Example usage:
 * <pre>{@code
 * WorkflowConfigurer.create()
 *                   .registerWorkflowIndex(config -> new JpaWorkflowIndex(
 *                           config.getComponent(JpaTransactionalExecutorProvider.class),
 *                           config.getComponent(Converter.class),
 *                           JpaWorkflowIndexConfiguration.DEFAULT
 *                   ));
 * }</pre>
 * <p>
 * Internal, because it is a storage detail of the {@link WorkflowEngine} that may change with it.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public interface WorkflowIndex {

    /**
     * Returns whether the rows of the given module hold every running workflow and every open wait, so a claim may read
     * them instead of sourcing the {@link RunningWorkflows} and the {@link WaitAssociations}.
     * <p>
     * A module is complete once {@link #store(String, WorkflowIndexChanges, ProcessingContext)} was given changes that
     * {@link WorkflowIndexChanges#complete() complete} it.
     *
     * @param module  the name of the workflow module
     * @param context the context to read in, or {@code null} to read in a transaction of its own
     * @return a future with {@code true} when the rows of the module are complete
     */
    CompletableFuture<Boolean> isComplete(String module, @Nullable ProcessingContext context);

    /**
     * Loads the rows of the given module whose workflow id the given segment owns.
     *
     * @param module  the name of the workflow module
     * @param segment the segment to load the rows of
     * @param context the context to read in, or {@code null} to read in a transaction of its own
     * @return a future with the running workflows and the open waits of the segment
     */
    CompletableFuture<WorkflowIndexRows> load(String module, Segment segment, @Nullable ProcessingContext context);

    /**
     * Stores the given changes to the rows of the given module, in their order.
     *
     * @param module  the name of the workflow module
     * @param changes the changes to store
     * @param context the context whose transaction the changes join, or {@code null} to store them in a transaction of
     *                their own
     * @return a future completing once the changes are stored
     */
    CompletableFuture<Void> store(String module, WorkflowIndexChanges changes, @Nullable ProcessingContext context);

    /**
     * Returns whether a {@link #store(String, WorkflowIndexChanges, ProcessingContext) store} in the context of a token
     * store write joins the transaction of that write, so the owner check of the token also guards the rows.
     * <p>
     * Only then does the engine store when a hibernated workflow wakes: that is a fact of one node, not of an event, so
     * a node that lost its segment must not be able to store it.
     *
     * @return {@code true} when rows and token are committed in one transaction
     */
    boolean storesWithToken();
}
