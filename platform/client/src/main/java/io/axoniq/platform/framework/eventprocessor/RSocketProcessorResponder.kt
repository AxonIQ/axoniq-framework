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

package io.axoniq.platform.framework.eventprocessor

import io.axoniq.platform.framework.api.ProcessorSegmentId
import io.axoniq.platform.framework.api.ProcessorStatusReport
import io.axoniq.platform.framework.api.ResetDecision
import io.axoniq.platform.framework.api.Routes
import io.axoniq.platform.framework.api.SegmentOverview
import io.axoniq.platform.framework.client.RSocketHandlerRegistrar
import org.slf4j.LoggerFactory

open class RSocketProcessorResponder(
        private val eventProcessorManager: EventProcessorManager,
        private val processorReportCreator: ProcessorReportCreator,
        private val registrar: RSocketHandlerRegistrar
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    fun start() {
        registrar.registerHandlerWithPayload(Routes.EventProcessor.START, String::class.java, this::handleStart)
        registrar.registerHandlerWithPayload(Routes.EventProcessor.STOP, String::class.java, this::handleStop)
        registrar.registerHandlerWithoutPayload(Routes.EventProcessor.STATUS, this::handleStatusQuery)
        registrar.registerHandlerWithPayload(Routes.EventProcessor.SEGMENTS, String::class.java, this::handleSegmentQuery)
        registrar.registerHandlerWithPayload(
                Routes.EventProcessor.RELEASE,
                ProcessorSegmentId::class.java,
                this::handleRelease
        )
        registrar.registerHandlerWithPayload(
                Routes.EventProcessor.SPLIT,
                ProcessorSegmentId::class.java,
                this::handleSplit
        )
        registrar.registerHandlerWithPayload(
                Routes.EventProcessor.MERGE,
                ProcessorSegmentId::class.java,
                this::handleMerge
        )
        registrar.registerHandlerWithPayload(
                Routes.EventProcessor.RESET,
                ResetDecision::class.java,
                this::handleReset
        )
        registrar.registerHandlerWithPayload(
                Routes.EventProcessor.CLAIM,
                ProcessorSegmentId::class.java,
                this::handleClaim
        )
    }

    private fun handleStart(processorName: String) {
        logger.debug("Handling Axoniq Platform START command for processor [{}]", processorName)
        eventProcessorManager.start(processorName)
    }

    private fun handleStop(processorName: String) {
        logger.debug("Handling Axoniq Platform STOP command for processor [{}]", processorName)
        eventProcessorManager.stop(processorName)
    }

    private fun handleStatusQuery(): ProcessorStatusReport {
        logger.debug("Handling Axoniq Platform STATUS query")
        return processorReportCreator.createReport()
    }

    private fun handleSegmentQuery(processor: String): SegmentOverview {
        logger.debug("Handling Axoniq Platform SEGMENTS query for processor [{}]", processor)
        return processorReportCreator.createSegmentOverview(processor)
    }

    fun handleRelease(processorSegmentId: ProcessorSegmentId) {
        logger.debug(
                "Handling Axoniq Platform RELEASE command for processor [{}] and segment [{}]",
                processorSegmentId.processorName,
                processorSegmentId.segmentId
        )
        eventProcessorManager.releaseSegment(processorSegmentId.processorName, processorSegmentId.segmentId)
    }

    private fun handleSplit(processorSegmentId: ProcessorSegmentId): Boolean {
        logger.debug(
                "Handling Axoniq Platform SPLIT command for processor [{}] and segment [{}]",
                processorSegmentId.processorName,
                processorSegmentId.segmentId
        )
        return eventProcessorManager
                .splitSegment(processorSegmentId.processorName, processorSegmentId.segmentId)
    }

    private fun handleMerge(processorSegmentId: ProcessorSegmentId): Boolean {
        logger.debug(
                "Handling Axoniq Platform MERGE command for processor [{}] and segment [{}]",
                processorSegmentId.processorName,
                processorSegmentId.segmentId
        )
        return eventProcessorManager
                .mergeSegment(processorSegmentId.processorName, processorSegmentId.segmentId)
    }

    private fun handleReset(resetDecision: ResetDecision) {
        logger.debug("Handling Axoniq Platform RESET command for processor [{}]", resetDecision.processorName)
        eventProcessorManager.resetTokens(resetDecision)
    }

    fun handleClaim(processorSegmentId: ProcessorSegmentId): Boolean {
        logger.debug(
                "Handling Axoniq Platform CLAIM command for processor [{}] and segment [{}]",
                processorSegmentId.processorName,
                processorSegmentId.segmentId
        )
        return eventProcessorManager.claimSegment(processorSegmentId.processorName, processorSegmentId.segmentId)
    }
}
