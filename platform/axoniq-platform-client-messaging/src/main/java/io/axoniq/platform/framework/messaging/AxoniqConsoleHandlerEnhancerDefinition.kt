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

import io.axoniq.platform.framework.api.metrics.PreconfiguredMetric
import io.github.oshai.kotlinlogging.KotlinLogging
import org.axonframework.common.Priority
import org.axonframework.messaging.core.Message
import org.axonframework.messaging.core.MessageStream
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition
import org.axonframework.messaging.core.annotation.MessageHandlingMember
import org.axonframework.messaging.core.annotation.WrappedMessageHandlingMember
import org.axonframework.messaging.core.unitofwork.ProcessingContext
import org.axonframework.messaging.queryhandling.annotation.QueryHandlingMember
import java.util.*

@Priority((Int.MIN_VALUE * 0.95).toInt())
class AxoniqConsoleHandlerEnhancerDefinition : HandlerEnhancerDefinition {
    private val logger = KotlinLogging.logger { }


    override fun <T : Any?> wrapHandler(original: MessageHandlingMember<T>): MessageHandlingMember<T> {
        if (original.attribute<Any>("EventSourcingHandler.payloadType").isPresent) {
            // Skip event sourcing handlers
            return original;
        }

        val declaringClassName = original.declaringClass().simpleName
        logger.debug { "Wrapping message handling member [$original] of class [$declaringClassName] for Axoniq Platform measurements." }
        return object : WrappedMessageHandlingMember<T>(original) {
            override fun handle(message: Message, context: ProcessingContext, target: T?): MessageStream<*> {
                HandlerMeasurement.onContext(context) {
                    logger.info { "Received message [${message.type()}] for class [$declaringClassName]" }
                    it.reportHandlingClass(declaringClassName)
                }
                val start = System.nanoTime()
                return super.handle(message, context, target)
                        .onNext {
                            logger.info { "onNext message [${message.type()}] in class [$declaringClassName]" }
                        }
                        .onComplete {
                            HandlerMeasurement.onContext(context) {
                                val end = System.nanoTime()
                                logger.info { "Registering handling time for message [${message.type()}] in class [$declaringClassName]: ${end - start} ns" }
                                it.registerMetricValue(
                                        metric = PreconfiguredMetric.MESSAGE_HANDLER_TIME,
                                        timeInNs = end - start
                                )
                            }
                        }
            }

            override fun <HT : Any?> unwrap(handlerType: Class<HT>): Optional<HT> {
                // The AnnotatedQueryHandlingComponent does an unwrap that we somehow need to intercept again
                if (handlerType == QueryHandlingMember::class.java) {
                    val unwrapped = super.unwrap(handlerType)
                    val value: QueryHandlingMember<*> = AxoniqPlatformQueryHandlingMember(unwrapped.get(), declaringClassName)
                    return Optional.of(value) as Optional<HT>
                }
                return super.unwrap(handlerType)
            }
        }
    }

}
