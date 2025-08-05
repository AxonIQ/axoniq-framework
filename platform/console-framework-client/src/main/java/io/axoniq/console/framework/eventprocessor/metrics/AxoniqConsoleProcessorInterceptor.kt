/*
 * Copyright (c) 2022-2025. AxonIQ B.V.
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

package io.axoniq.console.framework.eventprocessor.metrics

import io.axoniq.console.framework.messaging.AxoniqConsoleSpanFactory
import org.axonframework.eventhandling.EventMessage
import org.axonframework.messaging.InterceptorChain
import org.axonframework.messaging.Message
import org.axonframework.messaging.MessageHandlerInterceptor
import org.axonframework.messaging.unitofwork.BatchingUnitOfWork
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork
import org.axonframework.messaging.unitofwork.UnitOfWork
import org.axonframework.serialization.UnknownSerializedType
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.temporal.ChronoUnit

class AxoniqConsoleProcessorInterceptor(
        private val processorMetricsRegistry: ProcessorMetricsRegistry,
        private val processorName: String,
) : MessageHandlerInterceptor<Message<*>> {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun handle(unitOfWork: UnitOfWork<out Message<*>>, interceptorChain: InterceptorChain): Any? {
        val uow = CurrentUnitOfWork.map { it }.orElse(null)
        if (uow == null || unitOfWork.message.payload is UnknownSerializedType) {
            return interceptorChain.proceed()
        }
        val message = unitOfWork.message
        if (message !is EventMessage) {
            return interceptorChain.proceed()
        }
        try {
            AxoniqConsoleSpanFactory.onTopLevelSpanIfActive {
                it.reportProcessorName(processorName)
            }
            val segment = unitOfWork.resources()["Processor[$processorName]/SegmentId"] as? Int ?: -1
            val ingestTimestamp = Instant.now()
            processorMetricsRegistry.registerIngested(
                    processorName,
                    segment,
                    ChronoUnit.NANOS.between(message.timestamp, ingestTimestamp)
            )
            if (unitOfWork !is BatchingUnitOfWork<*> || unitOfWork.isFirstMessage) {
                unitOfWork.afterCommit {
                    processorMetricsRegistry.registerCommitted(
                            processorName,
                            segment,
                            ChronoUnit.NANOS.between(ingestTimestamp, Instant.now())
                    )
                }
            }

            return processorMetricsRegistry.doWithActiveMessageForSegment(processorName, segment, message.timestamp) {
                interceptorChain.proceed()
            }
        } catch (e: Exception) {
            logger.debug("AxonIQ Console could not register metrics for processor $processorName", e)
            return interceptorChain.proceed()
        }
    }
}
