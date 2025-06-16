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

package io.axoniq.console.framework.application

import io.axoniq.console.framework.api.ClientSettingsV2
import io.axoniq.console.framework.client.AxoniqConsoleRSocketClient
import io.axoniq.console.framework.client.ClientSettingsObserver
import io.axoniq.console.framework.client.ClientSettingsService
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class ApplicationMetricReporter(
        private val client: AxoniqConsoleRSocketClient,
        private val reportCreator: ApplicationReportCreator,
        private val clientSettingsService: ClientSettingsService,
        private val executor: ScheduledExecutorService,
) : ClientSettingsObserver {
    private var reportTask: ScheduledFuture<*>? = null
    private val logger = KotlinLogging.logger { }

    init {
        clientSettingsService.subscribeToSettings(this)
    }

    override fun onConnectedWithSettings(settings: ClientSettingsV2) {
        logger.debug { "Sending application information every ${settings.applicationReportInterval}ms to AxonIQ console" }
        this.reportTask = executor.scheduleWithFixedDelay({
            try {
                this.report()
            } catch (e: Exception) {
                logger.debug("Was unable to report application metrics: {}", e.message, e)
            }
        }, 0, settings.applicationReportInterval, TimeUnit.MILLISECONDS)
    }

    private fun report() {
        client.sendReport(io.axoniq.console.framework.api.Routes.Application.REPORT, reportCreator.createReport()).block()
    }

    override fun onDisconnected() {
        reportTask?.cancel(true)
        reportTask = null
    }
}

