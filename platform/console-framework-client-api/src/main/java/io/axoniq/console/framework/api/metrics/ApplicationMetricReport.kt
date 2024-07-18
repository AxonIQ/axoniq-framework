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

package io.axoniq.console.framework.api.metrics

data class ApplicationMetricReport(
        val availableProcessors: Int,
        val loadAverage: Double,
        val systemCpuUsage: Double,
        val processCpuUsage: Double,
        val maxOpenFiles: Long,
        val currentOpenFiles: Long,
        val memoryPools: Map<String, MemoryPoolReport>,
        val liveThreadCount: Int,
        val commandBus: BusMetricReport?,
        val queryBus: BusMetricReport?,
)

data class BusMetricReport(
        val capacity: Int,
        val usedCapacity: Double,
        val workQueueTimer: StatisticDistribution
)

data class MemoryPoolReport(
        val heap: Boolean,
        val used: Long,
        val committed: Long,
        val max: Long,
)
