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
