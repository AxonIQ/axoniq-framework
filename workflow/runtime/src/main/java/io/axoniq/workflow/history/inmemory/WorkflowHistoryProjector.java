/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.history.inmemory;

import io.axoniq.workflow.history.api.WorkflowHistory;
import io.axoniq.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.HashMap;

/**
 * Workflow history projector collecting historic information.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class WorkflowHistoryProjector implements EventHandler {

    private final MutableWorkflowHistoryRepository historyRepository;

    /**
     * Constructs a new workflow history projector using the history repository.
     *
     * @param historyRepository the history repository to use.
     */
    public WorkflowHistoryProjector(
            @Nonnull MutableWorkflowHistoryRepository historyRepository
    ) {
        this.historyRepository = historyRepository;
    }

    @Nonnull
    @Override
    public MessageStream.Empty<Message> handle(@Nonnull EventMessage event, @Nonnull ProcessingContext context) {
        if (MetadataUtils.hasWorkflowId().test(event.metadata())) {
            var workflowId = MetadataUtils.getWorkflowId(event.metadata());
            historyRepository.findById(workflowId).ifPresentOrElse(history -> {
                historyRepository.save(
                        new WorkflowHistory(workflowId, history.state().evolve(event, context))
                );
            }, () -> {
                historyRepository.save(new WorkflowHistory(workflowId,
                                                           new EventSourcedWorkflowState(
                                                                   new HashMap<>()
                                                           ).evolve(event, context)));
            });
        }
        return MessageStream.empty();
    }
}
