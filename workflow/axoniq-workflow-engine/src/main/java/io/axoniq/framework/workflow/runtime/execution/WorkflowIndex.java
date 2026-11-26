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

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Stores which workflow instances are running and which events they wait for, as rows a segment can select on its own.
 * <p>
 * On a claim, the {@link WorkflowEngine} reads the rows of the segment and then the lifecycle and wait events after the
 * token the segment resumes from, instead of sourcing every segment from the {@link WorkflowStore}.
 *
 * <h2>The index position is the token of the event processor</h2>
 * The index keeps no position of its own. The engine stores the row changes of an event before the processor stores a
 * token past that event, so the rows hold every change up to any stored token. Rows ahead of the token are harmless,
 * as applying the events after the token again gives the same rows. This holds only while every node that processes a
 * workflow module keeps the same index, and only the engine decides when the processor stores a token. So a module
 * with an index refuses to start when its processor has ordinary event handling components, stores its tokens in
 * another way, or keeps them in memory while the index survives a restart.
 *
 * <h2>Initialization</h2>
 * An index introduced while the processor already stored tokens, or one that was emptied, misses the workflows that
 * started before those tokens. So {@link #load(String, Segment, ProcessingContext) load} reports nothing for a module
 * until {@link #initialize(String, Set, Collection) initialize} marked it. A claim then sources every running workflow
 * and open wait of the module, and initializes the index with them. An application that resets the tokens of the
 * workflow processor must empty the index, its initialization included.
 *
 * <h2>Contract for implementations</h2>
 * The events stay the source of truth, so losing the index only makes claims slower. A store and an initialization
 * take effect as a whole or not at all. Storing a change twice has the same outcome as storing it once. An
 * initialization never removes a row, as another claim may already have stored a later change.
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
     * Loads the rows the given segment owns in the given module, or nothing when the module is not initialized.
     *
     * @param module  the name of the workflow module
     * @param segment the segment to load the rows of
     * @param context the context to read in, or {@code null} to read in a transaction of its own
     * @return a future with the running workflows and the open waits of the segment, empty when the module is not
     * initialized
     */
    CompletableFuture<Optional<WorkflowIndexRows>> load(String module,
                                                        Segment segment,
                                                        @Nullable ProcessingContext context);

    /**
     * Stores the given changes in their order.
     *
     * @param module  the name of the workflow module
     * @param changes the changes to store
     * @param context the context whose transaction the changes join, or {@code null} for a transaction of their own
     * @return a future completing once the changes are stored
     */
    CompletableFuture<Void> store(String module, WorkflowIndexChanges changes, @Nullable ProcessingContext context);

    /**
     * Adds the given running workflows and open waits of every segment of the module, and marks the module
     * initialized, in one transaction of its own. Rows that are stored already stay.
     *
     * @param module             the name of the workflow module
     * @param runningWorkflowIds the identifiers of every running workflow of the module
     * @param waits              every open wait of the module
     * @return a future completing once the rows and the mark are stored
     */
    CompletableFuture<Void> initialize(String module,
                                       Set<String> runningWorkflowIds,
                                       Collection<WaitAssociation> waits);

    /**
     * Returns whether a {@link #store(String, WorkflowIndexChanges, ProcessingContext) store} joins the transaction of
     * the token write, so the owner check of the token also guards the rows. Only then does the engine store that a
     * hibernated workflow woke, as that comes from one node, not from an event.
     *
     * @return {@code true} when rows and token are committed in one transaction
     */
    boolean storesWithToken();
}
