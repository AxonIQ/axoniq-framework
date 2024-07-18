/*
 * Copyright (c) 2024. AxonIQ B.V.
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

package io.axoniq.console.framework.application

import io.micrometer.core.instrument.Clock
import org.axonframework.axonserver.connector.PriorityRunnable
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