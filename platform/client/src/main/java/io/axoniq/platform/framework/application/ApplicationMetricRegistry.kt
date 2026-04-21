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

