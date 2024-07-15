/*
 * Copyright (c) 2022-2023. AxonIQ B.V.
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
import io.axoniq.console.framework.eventprocessor.metrics.ProcessorMetricsRegistry
import org.axonframework.common.ReflectionUtils
import org.axonframework.config.EventProcessingConfiguration
import org.axonframework.eventhandling.*
import org.axonframework.eventhandling.deadletter.DeadLetteringEventHandlerInvoker
import org.axonframework.eventhandling.pooled.PooledStreamingEventProcessor
import org.axonframework.messaging.deadletter.SequencedDeadLetterQueue

class ProcessorReportCreator(
        private val processingConfig: EventProcessingConfiguration,
        private val metricsRegistry: ProcessorMetricsRegistry,
) {

    fun createReport() = ProcessorStatusReport(
            processingConfig.eventProcessors()
                    .map { entry ->
                        if(entry.value is StreamingEventProcessor) {
                            val sep = entry.value as StreamingEventProcessor
                            ProcessorStatus(
                                    entry.key,
                                    entry.value.toProcessingGroupStatuses(),
                                    sep.tokenStoreIdentifier,
                                    sep.toType(),
                                    sep.isRunning,
                                    sep.isError,
                                    sep.maxCapacity(),
                                    sep.processingStatus().filterValues { !it.isErrorState }.size,
                                    sep.processingStatus().map { (_, segment) -> segment.toStatus(entry.key) },
                            )
                        } else {
                            val sep = entry.value as SubscribingEventProcessor
                            ProcessorStatus(
                                    sep.name,
                                    emptyList(),
                                    "",
                                    ProcessorMode.SUBSCRIBING,
                                    sep.isRunning,
                                    sep.isError,
                                    0,
                                    0,
                                    emptyList(),
                            )
                        }
                    }
    )

    fun createSegmentOverview(processorName: String): SegmentOverview {
        val tokenStore = processingConfig.tokenStore(processorName)
        val segments = tokenStore.fetchSegments(processorName)
        return SegmentOverview(
                segments.map { Segment.computeSegment(it, *segments) }
                        .map { SegmentDetails(it.segmentId, it.mergeableSegmentId(), it.mask) }
        )
    }

    private fun StreamingEventProcessor.toType(): ProcessorMode {
        return when (this) {
            is TrackingEventProcessor -> ProcessorMode.TRACKING
            is PooledStreamingEventProcessor -> ProcessorMode.POOLED
            else -> ProcessorMode.UNKNOWN
        }
    }

    private fun EventTrackerStatus.toStatus(name: String) = SegmentStatus(
            segment = this.segment.segmentId,
            mergeableSegment = this.segment.mergeableSegmentId(),
            mask = this.segment.mask,
            oneOf = this.segment.mask + 1,
            caughtUp = this.isCaughtUp,
            error = this.isErrorState,
            errorType = this.error?.javaClass?.typeName,
            errorMessage = this.error?.message,
            ingestLatency = metricsRegistry.ingestLatencyForProcessor(name, this.segment.segmentId).getValue(),
            commitLatency = metricsRegistry.commitLatencyForProcessor(name, this.segment.segmentId).getValue(),
            position = this.currentPosition?.orElse(-1) ?: -1,
            resetPosition = this.resetPosition?.orElse(-1) ?: -1,
    )

    private fun EventProcessor.toProcessingGroupStatuses(): List<ProcessingGroupStatus> =
            if (this is AbstractEventProcessor) {
                val invoker = this.eventHandlerInvoker()
                if (invoker is MultiEventHandlerInvoker) {
                    invoker.delegates().map { i -> i.toProcessingGroupStatus(this.name) }
                } else {
                    listOf(invoker.toProcessingGroupStatus(this.name))
                }
            } else {
                listOf(ProcessingGroupStatus(this.name, null))
            }

    private fun EventHandlerInvoker.toProcessingGroupStatus(processorName: String): ProcessingGroupStatus =
            if (this is DeadLetteringEventHandlerInvoker) {
                this.getStatusByReflectionOrDefault(processorName)
            } else {
                ProcessingGroupStatus(processorName, null)
            }

    private fun DeadLetteringEventHandlerInvoker.getStatusByReflectionOrDefault(processorName: String): ProcessingGroupStatus {
        val queueField = this.getField("queue") ?: return ProcessingGroupStatus(processorName, null)
        val queue = ReflectionUtils.getFieldValue<SequencedDeadLetterQueue<EventMessage<Any>>>(queueField, this)
        val processingGroupField = queue.getField("processingGroup")
                ?: return ProcessingGroupStatus(processorName, null)
        val processingGroup = ReflectionUtils.getFieldValue<String>(processingGroupField, queue)
        return ProcessingGroupStatus(processingGroup, queue.amountOfSequences())
    }

    private fun Any.getField(name: String) =
            this::class.java.declaredFields.firstOrNull { it.name == name }

}
