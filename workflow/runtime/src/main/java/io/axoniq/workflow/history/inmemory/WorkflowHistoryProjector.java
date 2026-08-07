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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Workflow history projector collecting historic information.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class WorkflowHistoryProjector implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowHistoryProjector.class);

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
            historyRepository.findById(workflowId)
                             .ifPresentOrElse(
                                     history ->
                                             historyRepository.save(
                                                     new WorkflowHistory(workflowId,
                                                                         history.state().evolve(event, context))
                                             ),
                                     () -> MetadataUtils.getWorkflowDefinitionId(event.metadata())
                                                        .ifPresentOrElse(
                                                                workflowDefinitionId -> historyRepository.save(
                                                                        new WorkflowHistory(
                                                                                workflowId,
                                                                                new EventSourcedWorkflowState(
                                                                                        workflowId,
                                                                                        new HashMap<>(),
                                                                                        workflowDefinitionId
                                                                                ).evolve(event, context)
                                                                        )
                                                                ),
                                                                () -> logger.debug(
                                                                        "Skipping history creation for workflow '{}': event '{}' carries no workflowDefinitionId "
                                                                                + "metadata and no history entry exists yet (its definition-carrying lifecycle "
                                                                                + "event lies before the replay window).",
                                                                        workflowId,
                                                                        event.type())
                                                        ));
        }
        return MessageStream.empty();
    }
}
