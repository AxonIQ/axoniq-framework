/*
 * Copyright (c) 2022-2024. AxonIQ B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.console.framework.eventprocessor

import io.axoniq.console.framework.api.*
import io.axoniq.console.framework.client.RSocketHandlerRegistrar
import org.axonframework.lifecycle.Lifecycle
import org.axonframework.lifecycle.Phase
import org.slf4j.LoggerFactory

open class RSocketDlqResponder(
    private val deadLetterManager: DeadLetterManager,
    private val registrar: RSocketHandlerRegistrar
) : Lifecycle {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun registerLifecycleHandlers(registry: Lifecycle.LifecycleRegistry) {
        registry.onStart(Phase.EXTERNAL_CONNECTIONS, this::start)
    }

    fun start() {
        registrar.registerHandlerWithPayload(
            Routes.ProcessingGroup.DeadLetter.LETTERS,
            DeadLetterRequest::class.java,
            this::handleDeadLetterQuery
        )
        registrar.registerHandlerWithPayload(
            Routes.ProcessingGroup.DeadLetter.SEQUENCE_SIZE,
            DeadLetterSequenceSize::class.java,
            this::handleSequenceSizeQuery
        )
        registrar.registerHandlerWithPayload(
            Routes.ProcessingGroup.DeadLetter.DELETE_SEQUENCE,
            DeadLetterSequenceDeleteRequest::class.java,
            this::handleDeleteSequenceCommand
        )
        registrar.registerHandlerWithPayload(
            Routes.ProcessingGroup.DeadLetter.DELETE_LETTER,
            DeadLetterSingleDeleteRequest::class.java,
            this::handleDeleteLetterCommand
        )
        registrar.registerHandlerWithPayload(
            Routes.ProcessingGroup.DeadLetter.PROCESS,
            DeadLetterProcessRequest::class.java,
            this::handleProcessCommand
        )
        registrar.registerHandlerWithPayload(
                Routes.ProcessingGroup.DeadLetter.PROCESS_ALL_SEQUENCES,
                ProcessAllDeadLetterSequencesRequest::class.java,
                this::handleProcessAllSequencesCommand
        )
        registrar.registerHandlerWithPayload(
                Routes.ProcessingGroup.DeadLetter.DELETE_ALL_SEQUENCES,
                DeleteAllDeadLetterSequencesRequest::class.java,
                this::handleDeleteAllSequencesCommand
        )
    }

    private fun handleDeadLetterQuery(request: DeadLetterRequest): DeadLetterResponse {
        logger.debug("Handling AxonIQ Console DEAD_LETTERS query for request [{}]", request)
        return deadLetterManager.deadLetters(request.processingGroup, request.offset, request.size)
    }

    private fun handleSequenceSizeQuery(request: DeadLetterSequenceSize): Long {
        logger.debug(
            "Handling AxonIQ Console DEAD_LETTER_SEQUENCE_SIZE query for processing group [{}]",
            request.processingGroup
        )
        return deadLetterManager.sequenceSize(request.processingGroup, request.sequenceIdentifier)
    }

    private fun handleDeleteSequenceCommand(request: DeadLetterSequenceDeleteRequest) {
        logger.debug(
            "Handling AxonIQ Console DELETE_FULL_DEAD_LETTER_SEQUENCE command for processing group [{}]",
            request.processingGroup
        )
        deadLetterManager.delete(request.processingGroup, request.sequenceIdentifier)
    }

    private fun handleDeleteLetterCommand(request: DeadLetterSingleDeleteRequest) {
        logger.debug(
            "Handling AxonIQ Console DELETE_DEAD_LETTER_IN_SEQUENCE command for processing group [{}]",
            request.processingGroup
        )
        deadLetterManager.delete(request.processingGroup, request.sequenceIdentifier, request.messageIdentifier)
    }

    private fun handleProcessCommand(request: DeadLetterProcessRequest): Boolean {
        logger.debug("Handling AxonIQ Console DEAD LETTERS query for processing group [{}]", request.processingGroup)
        return deadLetterManager.process(request.processingGroup, request.messageIdentifier)
    }

    private fun handleProcessAllSequencesCommand(request: ProcessAllDeadLetterSequencesRequest): Int {
        logger.debug("Handling AxonIQ Console PROCESS_ALL_DEAD_LETTER_SEQUENCES commands for processing group [{}]", request.processingGroup)
        return deadLetterManager.processAll(request.processingGroup, request.maxMessages)
    }

    private fun handleDeleteAllSequencesCommand(request: DeleteAllDeadLetterSequencesRequest): Int {
        logger.debug("Handling AxonIQ Console DELETE_ALL_DEAD_LETTER_SEQUENCES commands for processing group [{}]", request.processingGroup)
        return deadLetterManager.deleteAll(request.processingGroup, request.maxMessages)
    }

}
