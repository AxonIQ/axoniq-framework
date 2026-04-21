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

import io.axoniq.platform.framework.computeIfAbsentWithRetry
import org.axonframework.messaging.core.unitofwork.ProcessingContext
import java.time.Clock
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class ProcessorMetricsRegistry {
    private val ingestLatencyRegistry: MutableMap<String, MutableMap<Int, ExpiringLatencyValue>> = ConcurrentHashMap()
    private val commitLatencyRegistry: MutableMap<String, MutableMap<Int, ExpiringLatencyValue>> = ConcurrentHashMap()
    private val processingLatencyRegistry: MutableMap<String, MutableMap<Int, Instant?>> = ConcurrentHashMap()

    fun registerIngested(processor: String, segment: Int, latencyInNanos: Long) {
        ingestLatencyForProcessor(processor, segment).setValue(latencyInNanos.toDouble() / 1000000)
    }

    fun registerCommitted(processor: String, segment: Int, latencyInNanos: Long) {
        commitLatencyForProcessor(processor, segment).setValue(latencyInNanos.toDouble() / 1000000)
    }

    fun registerActiveMessage(processingContext: ProcessingContext, processor: String, segment: Int, messageTimestamp: Instant) {
        val processingMessageTimestampsForSegment = getProcessingLatencySegmentMap(processor)

        processingContext.runOnAfterCommit {
            processingMessageTimestampsForSegment.remove(segment)
        }
        processingContext.onError { _, _, _ ->
            processingMessageTimestampsForSegment.remove(segment)
        }
        processingMessageTimestampsForSegment[segment] = messageTimestamp
    }

    fun ingestLatencyForProcessor(processor: String, segment: Int): ExpiringLatencyValue {
        return ingestLatencyRegistry
                .computeIfAbsentWithRetry(processor) { ConcurrentHashMap() }
                .computeIfAbsentWithRetry(segment) { ExpiringLatencyValue() }
    }

    fun commitLatencyForProcessor(processor: String, segment: Int): ExpiringLatencyValue {
        return commitLatencyRegistry
                .computeIfAbsentWithRetry(processor) { ConcurrentHashMap() }
                .computeIfAbsentWithRetry(segment) { ExpiringLatencyValue() }
    }

    fun processingMessageLatencyForProcessor(processor: String, segment: Int): Long? {
        val processingTimestamp = getProcessingLatencySegmentMap(processor)
                .computeIfAbsentWithRetry(segment) { null }
        if (processingTimestamp == null) {
            return null
        }
        return ChronoUnit.MILLIS.between(processingTimestamp, Instant.now())
    }

    private fun getProcessingLatencySegmentMap(processor: String) = processingLatencyRegistry
            .computeIfAbsentWithRetry(processor) { ConcurrentHashMap() }

    class ExpiringLatencyValue(
            private val expiryTime: Long = 2 * 60 * 1000 // Default to 2 minutes
    ) {
        private val clock = Clock.systemUTC()
        private val value: AtomicReference<Double> = AtomicReference(-1.0)
        private val timeSet: AtomicLong = AtomicLong(-1)

        fun setValue(newValue: Double) {
            value.set(newValue)
            timeSet.set(clock.millis())
        }

        fun getValue(): Double {
            if (value.get() != null && clock.millis() - timeSet.get() < expiryTime) {
                return value.get()
            }
            return 0.0
        }
    }
}