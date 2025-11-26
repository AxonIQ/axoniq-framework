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

package io.axoniq.platform.framework.application

import io.axoniq.platform.framework.api.metrics.BusMetricReport

/**
 * Registry for application metrics. It holds the timers for the work queues of the query and command bus.
 * It also holds the decorators for the work queues of the query and command bus, if present in the application.
 */
class ApplicationMetricRegistry {
    private val busDecorators = mutableMapOf<BusType, MeasuringExecutorServiceDecorator>()

    fun getQueryBusMetrics() = getBusMetrics(BusType.QUERY)
    fun getCommandBusMetrics() = getBusMetrics(BusType.COMMAND)

    private fun getBusMetrics(type: BusType): BusMetricReport? {
        val decorator = busDecorators[type] ?: return null
        return BusMetricReport(
                capacity = decorator.getMaxCapacity(),
                usedCapacity = decorator.getUsedCapacity(),
        )
    }

    fun registerWorkQueueDecorator(busType: BusType, decorator: MeasuringExecutorServiceDecorator) {
        busDecorators[busType] = decorator
    }
}

