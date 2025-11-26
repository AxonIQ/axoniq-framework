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

package io.axoniq.platform.framework.messaging

import io.axoniq.platform.framework.api.ClientSettingsV2
import io.axoniq.platform.framework.api.Routes
import io.axoniq.platform.framework.AxoniqPlatformConfiguration
import io.axoniq.platform.framework.api.metrics.DispatcherStatisticIdentifier
import io.axoniq.platform.framework.api.metrics.DispatcherStatistics
import io.axoniq.platform.framework.api.metrics.DispatcherStatisticsWithIdentifier
import io.axoniq.platform.framework.api.metrics.HandlerStatistics
import io.axoniq.platform.framework.api.metrics.HandlerStatisticsMetricIdentifier
import io.axoniq.platform.framework.api.metrics.HandlerStatisticsWithIdentifier
import io.axoniq.platform.framework.api.metrics.HandlerType
import io.axoniq.platform.framework.api.metrics.MessageIdentifier
import io.axoniq.platform.framework.api.metrics.Metric
import io.axoniq.platform.framework.api.metrics.MetricTargetType
import io.axoniq.platform.framework.api.metrics.StatisticReport
import io.axoniq.platform.framework.client.AxoniqConsoleRSocketClient
import io.axoniq.platform.framework.client.ClientSettingsObserver
import io.axoniq.platform.framework.client.ClientSettingsService
import io.axoniq.platform.framework.computeIfAbsentWithRetry
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class HandlerMetricsRegistry(
        private val axoniqConsoleRSocketClient: AxoniqConsoleRSocketClient,
        private val clientSettingsService: ClientSettingsService,
        private val properties: AxoniqPlatformConfiguration
) : ClientSettingsObserver {
    private val logger = KotlinLogging.logger { }
    private var reportTask: ScheduledFuture<*>? = null
    private val meterRegistry = SimpleMeterRegistry()
    private val executor: ScheduledExecutorService = properties.reportingTaskExecutor

    private val dispatches: MutableMap<DispatcherStatisticIdentifier, RollingCountMeasure> = ConcurrentHashMap()
    private val handlers: MutableMap<HandlerStatisticsMetricIdentifier, HandlerRegistryStatistics> = ConcurrentHashMap()

    private val noHandlerIdentifier = HandlerStatisticsMetricIdentifier(
        HandlerType.Origin,
        "application",
        MessageIdentifier("Dispatcher", properties.applicationName)
    )

    init {
        clientSettingsService.subscribeToSettings(this)
    }

    override fun onConnectedWithSettings(settings: ClientSettingsV2) {
        logger.debug { "Sending handler information every ${settings.handlerReportInterval}ms to Axoniq Platform" }
        this.reportTask = executor.scheduleAtFixedRate({
            try {
                val stats = getStats()
                logger.debug { "${"Sending metrics: {}"} $stats" }
                axoniqConsoleRSocketClient.sendReport(Routes.MessageFlow.STATS, stats).block()
            } catch (e: Exception) {
                logger.warn { "${"No metrics could be reported to Axoniq Platform: {}"} ${e.message}" }
            }
        }, 0, settings.handlerReportInterval, TimeUnit.MILLISECONDS)
    }

    override fun onDisconnected() {
        this.reportTask?.cancel(true)
        this.reportTask = null
    }

    private fun getStats(): StatisticReport {
        val flow = StatisticReport(
            handlers = handlers.entries
                .map {
                    HandlerStatisticsWithIdentifier(
                        it.key, HandlerStatistics(
                            it.value.totalCount.count(),
                            it.value.failureCount.count(),
                            it.value.totalTimer.takeSnapshot().toDistribution(),
                            it.value.metrics.map { (k, v) -> k.fullIdentifier to v.takeSnapshot().toDistribution() }
                                .toMap()
                        )
                    )
                } + dispatches.filter { it.key.handlerInformation?.type == HandlerType.Origin }.map {
                HandlerStatisticsWithIdentifier(
                    it.key.handlerInformation!!,
                    HandlerStatistics(0.0, 0.0, null, emptyMap())
                )
            },
            dispatchers = dispatches.entries
                .map {
                    DispatcherStatisticsWithIdentifier(
                        it.key,
                        DispatcherStatistics(it.value.count())
                    )
                },
            aggregates = emptyList()
        )
        return flow
    }

    private fun createTimer(handler: Any, name: String): Timer {
        return io.axoniq.platform.framework.createTimer(meterRegistry, "${handler}_timer_$name")
    }

    fun registerMeasurement(
            measurement: HandlerMeasurement
    ) {
        val handler = HandlerStatisticsMetricIdentifier(
            type = measurement.handlerType,
            component = measurement.componentName() ?: "Lambda",
            message = measurement.message.toInformation()
        )
        registerMessageHandled(
                handler = handler,
                success = measurement.isSuccessful(),
                duration = (measurement.getCompletedTime() ?: System.nanoTime()) - measurement.startTime,
                metrics = measurement.getRegisteredMetrics()
        )
        measurement.getAllDispatchedMessages().forEach {
            registerMessageDispatchedDuringHandling(
                DispatcherStatisticIdentifier(handlerInformation = handler, message = it)
            )
        }
    }

    fun registerMessageHandled(
        handler: HandlerStatisticsMetricIdentifier,
        success: Boolean,
        duration: Long,
        metrics: Map<Metric, Long>
    ) {
        val handlerStats = handlers.computeIfAbsentWithRetry(handler) { _ ->
            HandlerRegistryStatistics(createTimer(handler, "total"))
        }
        handlerStats.totalTimer.record(duration, TimeUnit.NANOSECONDS)
        metrics.filter { it.key.targetTypes.contains(MetricTargetType.HANDLER) }
                .forEach { (metric, value) ->
                    handlerStats.metrics
                            .computeIfAbsentWithRetry(metric) { createTimer(handler, metric.fullIdentifier) }
                            .record(value, metric.type.distributionUnit)
                }

        handlerStats.totalCount.increment()
        if (!success) {
            handlerStats.failureCount.increment()
        }
    }

    fun registerMessageDispatchedDuringHandling(
        dispatcher: DispatcherStatisticIdentifier,
    ) {
        dispatches.computeIfAbsentWithRetry(dispatcher) { _ ->
            RollingCountMeasure()
        }.increment()
    }

    fun registerMessageDispatchedWithoutHandling(
        message: MessageIdentifier,
    ) {
        dispatches.computeIfAbsentWithRetry(DispatcherStatisticIdentifier(noHandlerIdentifier, message)) { _ ->
            RollingCountMeasure()
        }.increment()
    }

    /**
     * Holder object of a handler and all its related stats.
     * Includes total time, and the broken down metrics
     */
    private data class HandlerRegistryStatistics(
            val totalTimer: Timer,
            val totalCount: RollingCountMeasure = RollingCountMeasure(),
            val failureCount: RollingCountMeasure = RollingCountMeasure(),
            val metrics: MutableMap<Metric, Timer> = ConcurrentHashMap()
    )
}

