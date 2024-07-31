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

import io.axoniq.console.framework.api.ProcessorSegmentId
import io.axoniq.console.framework.api.ProcessorStatusReport
import io.axoniq.console.framework.api.ResetDecision
import io.axoniq.console.framework.api.SegmentOverview
import io.axoniq.console.framework.client.RSocketHandlerRegistrar
import org.axonframework.lifecycle.Lifecycle
import org.axonframework.lifecycle.Phase
import org.slf4j.LoggerFactory

open class RSocketProcessorResponder(
    private val eventProcessorManager: EventProcessorManager,
    private val processorReportCreator: ProcessorReportCreator,
    private val registrar: RSocketHandlerRegistrar
) : Lifecycle {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun registerLifecycleHandlers(registry: Lifecycle.LifecycleRegistry) {
        registry.onStart(Phase.EXTERNAL_CONNECTIONS, this::start)
    }

    fun start() {
        registrar.registerHandlerWithPayload(io.axoniq.console.framework.api.Routes.EventProcessor.START, String::class.java, this::handleStart)
        registrar.registerHandlerWithPayload(io.axoniq.console.framework.api.Routes.EventProcessor.STOP, String::class.java, this::handleStop)
        registrar.registerHandlerWithoutPayload(io.axoniq.console.framework.api.Routes.EventProcessor.STATUS, this::handleStatusQuery)
        registrar.registerHandlerWithPayload(io.axoniq.console.framework.api.Routes.EventProcessor.SEGMENTS, String::class.java, this::handleSegmentQuery)
        registrar.registerHandlerWithPayload(
            io.axoniq.console.framework.api.Routes.EventProcessor.RELEASE,
            ProcessorSegmentId::class.java,
            this::handleRelease
        )
        registrar.registerHandlerWithPayload(
            io.axoniq.console.framework.api.Routes.EventProcessor.SPLIT,
            ProcessorSegmentId::class.java,
            this::handleSplit
        )
        registrar.registerHandlerWithPayload(
            io.axoniq.console.framework.api.Routes.EventProcessor.MERGE,
            ProcessorSegmentId::class.java,
            this::handleMerge
        )
        registrar.registerHandlerWithPayload(
            io.axoniq.console.framework.api.Routes.EventProcessor.RESET,
            ResetDecision::class.java,
            this::handleReset
        )
        registrar.registerHandlerWithPayload(
            io.axoniq.console.framework.api.Routes.EventProcessor.CLAIM,
            ProcessorSegmentId::class.java,
            this::handleClaim
        )
    }

    private fun handleStart(processorName: String) {
        logger.debug("Handling AxonIQ Console START command for processor [{}]", processorName)
        eventProcessorManager.start(processorName)
    }

    private fun handleStop(processorName: String) {
        logger.debug("Handling AxonIQ Console STOP command for processor [{}]", processorName)
        eventProcessorManager.stop(processorName)
    }

    private fun handleStatusQuery(): ProcessorStatusReport {
        logger.debug("Handling AxonIQ Console STATUS query")
        return processorReportCreator.createReport()
    }

    private fun handleSegmentQuery(processor: String): SegmentOverview {
        logger.debug("Handling AxonIQ Console SEGMENTS query for processor [{}]", processor)
        return processorReportCreator.createSegmentOverview(processor)
    }

    fun handleRelease(processorSegmentId: ProcessorSegmentId) {
        logger.debug(
            "Handling AxonIQ Console RELEASE command for processor [{}] and segment [{}]",
            processorSegmentId.processorName,
            processorSegmentId.segmentId
        )
        eventProcessorManager.releaseSegment(processorSegmentId.processorName, processorSegmentId.segmentId)
    }

    private fun handleSplit(processorSegmentId: ProcessorSegmentId): Boolean {
        logger.debug(
            "Handling AxonIQ Console SPLIT command for processor [{}] and segment [{}]",
            processorSegmentId.processorName,
            processorSegmentId.segmentId
        )
        return eventProcessorManager
            .splitSegment(processorSegmentId.processorName, processorSegmentId.segmentId)
    }

    private fun handleMerge(processorSegmentId: ProcessorSegmentId): Boolean {
        logger.debug(
            "Handling AxonIQ Console MERGE command for processor [{}] and segment [{}]",
            processorSegmentId.processorName,
            processorSegmentId.segmentId
        )
        return eventProcessorManager
            .mergeSegment(processorSegmentId.processorName, processorSegmentId.segmentId)
    }

    private fun handleReset(resetDecision: ResetDecision) {
        logger.debug("Handling AxonIQ Console RESET command for processor [{}]", resetDecision.processorName)
        eventProcessorManager.resetTokens(resetDecision)
    }

    fun handleClaim(processorSegmentId: ProcessorSegmentId): Boolean {
        logger.debug(
            "Handling AxonIQ Console CLAIM command for processor [{}] and segment [{}]",
            processorSegmentId.processorName,
            processorSegmentId.segmentId
        )
        return eventProcessorManager.claimSegment(processorSegmentId.processorName, processorSegmentId.segmentId)
    }
}
