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
import io.github.oshai.kotlinlogging.KotlinLogging
import org.axonframework.common.infra.ComponentDescriptor
import org.axonframework.messaging.commandhandling.CommandBus
import org.axonframework.messaging.commandhandling.CommandHandler
import org.axonframework.messaging.commandhandling.CommandMessage
import org.axonframework.messaging.commandhandling.CommandResultMessage
import io.axoniq.framework.messaging.commandhandling.distributed.DistributedCommandBusConfigurationEnhancer
import org.axonframework.messaging.core.QualifiedName
import org.axonframework.messaging.core.unitofwork.ProcessingContext
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean

class AxoniqPlatformCommandBus(
        private val delegate: CommandBus,
        private val metricsRegistry: HandlerMetricsRegistry,
) : CommandBus {
    private val logger = KotlinLogging.logger { }

    companion object {
        const val DECORATION_ORDER = DistributedCommandBusConfigurationEnhancer.DISTRIBUTED_COMMAND_BUS_ORDER - 1
    }

    override fun dispatch(command: CommandMessage, processingContext: ProcessingContext?): CompletableFuture<CommandResultMessage> {
        val measurementContainer = processingContext?.getResource(HandlerMeasurement.RESOURCE_KEY)
        if (measurementContainer == null) {
            metricsRegistry.registerMessageDispatchedWithoutHandling(command.toInformation())
        } else {
            measurementContainer.reportMessageDispatched(command.toInformation())
        }

        return delegate.dispatch(command, processingContext)
    }

    override fun subscribe(name: QualifiedName, commandHandler: CommandHandler): CommandBus {
        logger.debug { "Decorating command handler subscription for [$name] and handler [$commandHandler]" }
        delegate.subscribe(name) { command, context ->
            logger.debug { "Handling command [${command.type()}] with handler [$commandHandler]" }
            val measurement = HandlerMeasurement(message = command, handlerType = HandlerType.Message)
            context.putResource(HandlerMeasurement.RESOURCE_KEY, measurement)
            val ranAfterCommit = AtomicBoolean()
            context.runOnAfterCommit {
                ranAfterCommit.set(true)
                logger.debug { "Registering success for command [${command.type()}] with handler [$commandHandler]" }
                measurement.complete(true)
                metricsRegistry.registerMeasurement(measurement)
            }
            context.onError { _, _, _ ->
                if (!ranAfterCommit.get()) {
                    logger.debug { "Registering failure for command [${command.type()}] with handler [$commandHandler]" }
                    measurement.complete(false)
                    metricsRegistry.registerMeasurement(measurement)
                }
            }
            commandHandler.handle(command, context)
        }
        return this
    }

    override fun describeTo(descriptor: ComponentDescriptor) {
        descriptor.describeWrapperOf(delegate)
    }
}