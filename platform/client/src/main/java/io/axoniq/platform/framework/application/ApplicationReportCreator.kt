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

import io.axoniq.platform.framework.api.metrics.ApplicationMetricReport
import io.axoniq.platform.framework.api.metrics.MemoryPoolReport
import java.lang.management.ManagementFactory
import java.lang.management.MemoryType

class ApplicationReportCreator(
        private val registry: ApplicationMetricRegistry,
) {
    private val memoryBeans = ManagementFactory.getMemoryPoolMXBeans()
    private val threadBean = ManagementFactory.getThreadMXBean()
    private val osBean = ManagementFactory.getOperatingSystemMXBean()
    private val cpuMetricsProvider = CpuMetricsProvider()

    fun createReport(): ApplicationMetricReport {
        return ApplicationMetricReport(
                loadAverage = osBean.systemLoadAverage,
                processCpuUsage = cpuMetricsProvider.getProcessCpuUsage(),
                systemCpuUsage = cpuMetricsProvider.getSystemCpuUsage(),
                heapUsage = determineMemoryUsage(),
                liveThreadCount = threadBean.threadCount,
                commandBus = registry.getCommandBusMetrics(),
                queryBus = registry.getQueryBusMetrics()
        )
    }

    private fun determineMemoryUsage(): MemoryPoolReport {
        return memoryBeans
                .filter { it.type == MemoryType.HEAP && it.usage.max > 0 }
                .fold(MemoryPoolReport(0.0, 0.0, 0.0)) { acc, bean ->
                    MemoryPoolReport(
                            acc.committed + bean.usage.used.toMb(),
                            acc.used + bean.usage.committed.toMb(),
                            acc.max + bean.usage.max.toMb(),
                    )
                }
    }

    private fun Number.toMb(): Double = this.toDouble() / 1024 / 1024
}
