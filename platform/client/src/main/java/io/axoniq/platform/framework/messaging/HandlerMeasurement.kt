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

package io.axoniq.platform.framework.messaging

import io.axoniq.platform.framework.api.metrics.HandlerType
import io.axoniq.platform.framework.api.metrics.MessageIdentifier
import io.axoniq.platform.framework.api.metrics.Metric
import io.github.oshai.kotlinlogging.KotlinLogging
import org.axonframework.messaging.core.Context
import org.axonframework.messaging.core.Message

class HandlerMeasurement(
        val message: Message,
        val handlerType: HandlerType,
        val predeterminedComponentName: String? = null,
        val startTime: Long = System.nanoTime()
) {
    private var completedTime: Long? = null
    private var success: Boolean = true
    private var handlingClass: String? = null
    private val registeredMetrics: MutableMap<Metric, Long> = mutableMapOf()
    private val dispatchedMessages: MutableList<MessageIdentifier> = mutableListOf()


    fun getRegisteredMetrics(): Map<Metric, Long> = registeredMetrics.toMap()

    fun getAllDispatchedMessages(): List<MessageIdentifier> {
        return dispatchedMessages.toList()
    }

    fun isSuccessful(): Boolean = success

    fun getCompletedTime(): Long? = completedTime

    fun componentName(): String = predeterminedComponentName ?: handlingClass ?: "Lambda"

    fun complete(successful: Boolean) {
        if (completedTime != null) {
            logger.warn { "HandlerMeasurement for handler [${message.type()}] is already completed. Can not complete gain. Ignoring." }
            return
        }
        completedTime = System.nanoTime()
        this.success = successful
    }

    fun registerMetricValue(metric: Metric, timeInNs: Long) {
        if (completedTime != null) {
            logger.warn { "HandlerMeasurement for handler [${message.type()}] is already completed. Can not register metric [$metric] with value [$timeInNs]. Ignoring." }
            return
        }
        registeredMetrics.compute(metric) { _, it ->
            // Sum the metric if it was already registered
            (it ?: 0L) + timeInNs
        }
    }

    fun reportMessageDispatched(messageIdentifier: MessageIdentifier) {
        if (completedTime != null) {
            logger.warn { "HandlerMeasurement for handler [${message.type()}] is already completed. Can not report dispatched message [$messageIdentifier]. Ignoring." }
            return
        }
        dispatchedMessages.add(messageIdentifier)
    }

    fun reportHandlingClass(handlingClass: String) {
        this.handlingClass = handlingClass
    }

    companion object {
        private val logger = KotlinLogging.logger { }
        val RESOURCE_KEY: Context.ResourceKey<HandlerMeasurement> = Context.ResourceKey.withLabel<HandlerMeasurement>("Axoniq Platform")

        fun fromContext(context: Context): HandlerMeasurement? {
            return context.getResource(RESOURCE_KEY)
        }

        fun onContext(context: Context, block: (HandlerMeasurement) -> Unit) {
            fromContext(context)?.apply(block)
        }
    }
}