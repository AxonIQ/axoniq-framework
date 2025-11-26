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

import io.axoniq.platform.framework.messaging.enhancing.AxoniqPlatformCommandHandlingMember
import io.axoniq.platform.framework.messaging.enhancing.AxoniqPlatformMessageHandlingMember
import io.axoniq.platform.framework.messaging.enhancing.AxoniqPlatformQueryHandlingMember
import io.github.oshai.kotlinlogging.KotlinLogging
import org.axonframework.common.Priority
import org.axonframework.messaging.commandhandling.annotation.CommandHandlingMember
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition
import org.axonframework.messaging.core.annotation.MessageHandlingMember
import org.axonframework.messaging.queryhandling.annotation.QueryHandlingMember

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
        if (original is QueryHandlingMember<*>) {
            val axoniqMember = AxoniqPlatformQueryHandlingMember(original, declaringClassName)
            return axoniqMember as MessageHandlingMember<T>
        } else if (original is CommandHandlingMember<*>) {
            return AxoniqPlatformCommandHandlingMember(original as CommandHandlingMember<T>, declaringClassName)
        }
        return AxoniqPlatformMessageHandlingMember(original, declaringClassName)
    }

}
