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
package io.axoniq.framework.workflow.simulation.harness;

import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.query.utils.WorkflowStateQueryMatcher;
import org.axonframework.common.FutureUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A history repository whose query-side reads can be frozen at a point in time, so a scenario can hold the
 * projection the {@code WorkflowManager} reads at a chosen lag while the {@code WorkflowHistoryProjector} keeps
 * writing to it.
 * <p>
 * The projector reads through {@link #findById(String)} and writes through {@link #save(WorkflowHistory)}; both go
 * straight to the delegate so the projection itself stays consistent. Only {@link #findAll(WorkflowStateQuery)}, the
 * read {@code SimpleWorkflowManager} uses, answers from the snapshot taken by {@link #hold()} until {@link #release()}.
 * The snapshot holds {@link FrozenWorkflowState} copies: the projector evolves its stored state objects in place, so
 * a snapshot of references would follow it.
 * <p>
 * This is the deterministic twin of the processor lag the real engine has: the manager answers from a projection that
 * is behind the log by an amount nothing in the engine bounds. Production is untouched; the seam is the repository
 * component the harness already owns.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class LaggingHistoryRepository implements MutableWorkflowHistoryRepository {

    private final InMemoryWorkflowHistoryRepository delegate = new InMemoryWorkflowHistoryRepository();
    private volatile @org.jspecify.annotations.Nullable List<WorkflowHistory> heldSnapshot = null;

    /**
     * Freezes the query-side view at the projection's current content.
     */
    public void hold() {
        heldSnapshot = FutureUtils.joinAndUnwrap(delegate.findAll()).stream()
                                  .map(history -> new WorkflowHistory(history.workflowId(),
                                                                      new FrozenWorkflowState(history.state())))
                                  .toList();
    }

    /**
     * Lets the query-side view follow the projection again.
     */
    public void release() {
        heldSnapshot = null;
    }

    /**
     * Whether the query-side view is currently frozen.
     *
     * @return {@code true} while {@link #hold()} is in effect.
     */
    public boolean isHeld() {
        return heldSnapshot != null;
    }

    @Override
    public CompletableFuture<List<WorkflowHistory>> findAll(WorkflowStateQuery query) {
        var snapshot = heldSnapshot;
        if (snapshot == null) {
            return delegate.findAll(query);
        }
        return CompletableFuture.completedFuture(
                snapshot.stream()
                        .filter(history -> WorkflowStateQueryMatcher.matches(query, history.state()))
                        .toList());
    }

    @Override
    public CompletableFuture<Optional<WorkflowHistory>> findById(String workflowId) {
        return delegate.findById(workflowId);
    }

    @Override
    public void save(WorkflowHistory workflowHistory) {
        delegate.save(workflowHistory);
    }

    @Override
    public void clear() {
        delegate.clear();
    }
}
