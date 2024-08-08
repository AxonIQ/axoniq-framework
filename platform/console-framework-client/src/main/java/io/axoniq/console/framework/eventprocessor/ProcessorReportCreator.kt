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
    companion object {
        const val MULTI_TENANT_PROCESSOR_CLASS = "org.axonframework.extensions.multitenancy.components.eventhandeling.MultiTenantEventProcessor"
    }

    fun createReport() = ProcessorStatusReport(
            processingConfig.eventProcessors()
                    .flatMap { entry ->
                        when (val processor = entry.value) {
                            is StreamingEventProcessor -> listOf(streamingStatus(entry.key, processor))
                            is SubscribingEventProcessor -> listOf(subscribingStatus(entry.key, processor))
                            else -> ignoreMultiTenantProcessorOrDefault(entry.key, processor)
                        }
                    }
    )

    private fun streamingStatus(name: String, processor: StreamingEventProcessor) =
            ProcessorStatus(
                    name,
                    processor.toProcessingGroupStatuses(),
                    processor.tokenStoreIdentifier,
                    processor.toType(),
                    processor.isRunning,
                    processor.isError,
                    processor.maxCapacity(),
                    processor.processingStatus().filterValues { !it.isErrorState }.size,
                    processor.processingStatus().map { (_, segment) -> segment.toStatus(name) },
            )

    private fun subscribingStatus(name: String, processor: SubscribingEventProcessor) =
            ProcessorStatus(
                    name,
                    emptyList(),
                    "",
                    ProcessorMode.SUBSCRIBING,
                    processor.isRunning,
                    processor.isError,
                    0,
                    0,
                    emptyList(),
            )

    private fun defaultStatus(name: String, processor: EventProcessor) =
            ProcessorStatus(
                    name,
                    emptyList(),
                    "",
                    ProcessorMode.UNKNOWN,
                    processor.isRunning,
                    processor.isError,
                    0,
                    0,
                    emptyList(),
            )

    /**
     * In the case of the multi tenant event processor, they are also registered individually. So we can ignore the
     * combining event processor containing a map which each individual tenant.
     */
    private fun ignoreMultiTenantProcessorOrDefault(name: String, processor: EventProcessor): List<ProcessorStatus> {
        return if (processor.javaClass.name == MULTI_TENANT_PROCESSOR_CLASS) {
            emptyList()
        } else listOf(defaultStatus(name, processor))
    }

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
