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

package io.axoniq.platform.framework.application

import io.axoniq.platform.framework.metrics.SlidingTimeWindowReservoir
import io.micrometer.core.instrument.Clock
import org.axonframework.common.util.PriorityRunnable
import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * Measures an ExecutorService for a bus in Axon Framework. Extracts the current capacity, max capacity and
 * queue timer of tasks.
 */
class MeasuringExecutorServiceDecorator(
        private val busType: BusType,
        private val delegate: ExecutorService,
        private val applicationMetricRegistry: ApplicationMetricRegistry
) : ExecutorService by delegate {
    private val clock = Clock.SYSTEM
    private val monitor = SlidingTimeWindowReservoir(1, TimeUnit.MINUTES, clock)

    init {
        applicationMetricRegistry.registerWorkQueueDecorator(busType, this)
    }

    override fun execute(command: Runnable) {
        if (command !is PriorityRunnable) {
            delegate.execute(command)
            return
        }
        val instrumentedRunnable = PriorityRunnable({
            val start: Long = clock.monotonicTime()
            try {
                command.run()
            } finally {
                monitor.update(clock.monotonicTime() - start)
            }
        }, command.priority(), command.sequence())
        return delegate.execute(instrumentedRunnable)
    }

    fun getMaxCapacity(): Int {
        if (delegate is ThreadPoolExecutor) {
            return delegate.maximumPoolSize
        }

        return 1
    }

    fun getUsedCapacity(): Double {
        val totalProcessTime = monitor.measurements.stream().reduce(0L) { a: Long, b: Long -> a + b } as Long
        return totalProcessTime.toDouble() / TimeUnit.MINUTES.toNanos(1).toDouble()
    }
}