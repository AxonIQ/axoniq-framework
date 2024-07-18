/*
 * Copyright (c) 2022-2024. AxonIQ B.V.
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

import io.axoniq.console.framework.api.metrics.ApplicationMetricReport
import io.axoniq.console.framework.api.metrics.MemoryPoolReport
import java.lang.management.ManagementFactory
import java.lang.management.MemoryType

class ApplicationReportCreator(
        private val registry: ApplicationMetricRegistry,
) {
    private val memoryBeans = ManagementFactory.getMemoryPoolMXBeans()
    private val threadBean = ManagementFactory.getThreadMXBean()
    private val osBean = ManagementFactory.getOperatingSystemMXBean()
    private val cpuMetricsProvider = CpuMetricsProvider()
    private val fileDescriptorProvider = FileDescriptorProvider()

    fun createReport(): ApplicationMetricReport {
        return ApplicationMetricReport(
                availableProcessors = osBean.availableProcessors,
                loadAverage = osBean.systemLoadAverage,
                processCpuUsage = cpuMetricsProvider.getProcessCpuUsage(),
                systemCpuUsage = cpuMetricsProvider.getSystemCpuUsage(),
                currentOpenFiles = fileDescriptorProvider.getOpenFileDescriptors(),
                maxOpenFiles = fileDescriptorProvider.getMaxOpenFileDescriptors(),
                memoryPools = memoryBeans.associate {
                    it.name to MemoryPoolReport(
                            heap = it.type == MemoryType.HEAP,
                            used = it.usage.used,
                            committed = it.usage.committed,
                            max = it.usage.max
                    )
                },
                liveThreadCount = threadBean.threadCount,
                commandBus = registry.getCommandBusMetrics(),
                queryBus = registry.getQueryBusMetrics()
        )
    }
}
