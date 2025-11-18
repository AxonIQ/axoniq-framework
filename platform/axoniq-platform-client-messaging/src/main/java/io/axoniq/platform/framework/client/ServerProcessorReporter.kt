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

package io.axoniq.platform.framework.client

import io.axoniq.platform.framework.api.ClientSettingsV2
import io.axoniq.platform.framework.api.Routes
import io.axoniq.platform.framework.AxoniqPlatformProperties
import io.axoniq.platform.framework.eventprocessor.ProcessorReportCreator
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class ServerProcessorReporter(
        private val client: AxoniqConsoleRSocketClient,
        private val processorReportCreator: ProcessorReportCreator,
        private val clientSettingsService: ClientSettingsService,
        private val properties: AxoniqPlatformProperties
) : ClientSettingsObserver {
    private var reportTask: ScheduledFuture<*>? = null
    private val logger = KotlinLogging.logger { }
    private val executor = properties.reportingTaskExecutor

    init {
        clientSettingsService.subscribeToSettings(this)
    }

    override fun onConnectedWithSettings(settings: ClientSettingsV2) {
        logger.info { "Sending processor information every ${settings.processorReportInterval}ms to Axoniq Platform" }
        this.reportTask = executor.scheduleWithFixedDelay({
            try {
                this.report()
            } catch (e: Exception) {
                logger.debug("Was unable to report processor metrics: {}", e.message, e)
            }
        }, 0, settings.processorReportInterval, TimeUnit.MILLISECONDS)
    }

    private fun report() {
        client.sendReport(Routes.EventProcessor.REPORT, processorReportCreator.createReport()).block()
    }

    override fun onDisconnected() {
        reportTask?.cancel(true)
        reportTask = null
    }
}

