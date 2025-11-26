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

package io.axoniq.platform.framework.messaging.enhancing

import io.axoniq.platform.framework.api.metrics.PreconfiguredMetric
import io.axoniq.platform.framework.messaging.HandlerMeasurement
import io.github.oshai.kotlinlogging.KotlinLogging
import org.axonframework.messaging.core.Message
import org.axonframework.messaging.core.MessageStream
import org.axonframework.messaging.core.annotation.MessageHandlingMember
import org.axonframework.messaging.core.unitofwork.ProcessingContext
import java.util.Optional

open class AxoniqPlatformMessageHandlingMember<T>(open val delegate: MessageHandlingMember<T>, val declaringClassName: String) : MessageHandlingMember<T> {
    private val logger = KotlinLogging.logger { }

    override fun payloadType(): Class<*>? {
        return delegate.payloadType()
    }

    override fun canHandle(message: Message, context: ProcessingContext): Boolean {
        return delegate.canHandle(message, context)
    }

    override fun canHandleMessageType(messageType: Class<out Message?>): Boolean {
        return delegate.canHandleMessageType(messageType)
    }

    @Suppress("DEPRECATION")
    override fun handleSync(message: Message, context: ProcessingContext, target: T): Any? {
        return delegate.handleSync(message, context, target)
    }

    override fun handle(message: Message, context: ProcessingContext, target: T): MessageStream<*>? {
        HandlerMeasurement.onContext(context) {
            logger.debug { "Received message [${message.type()}] for class [$declaringClassName]" }
            it.reportHandlingClass(declaringClassName)
        }
        val start = System.nanoTime()
        val stream = delegate.handle(message, context, target)
        HandlerMeasurement.onContext(context) {
            val end = System.nanoTime()
            logger.debug { "Registering handling time for message [${message.type()}] in class [$declaringClassName]: ${end - start} ns" }
            it.registerMetricValue(
                    metric = PreconfiguredMetric.MESSAGE_HANDLER_TIME,
                    timeInNs = end - start
            )
        }
        return stream
    }

    override fun <HT : Any?> unwrap(handlerType: Class<HT>): Optional<HT> {
        if(handlerType.isInstance(this)) {
            return Optional.of(this) as Optional<HT>
        }
        return delegate.unwrap(handlerType)
    }

}