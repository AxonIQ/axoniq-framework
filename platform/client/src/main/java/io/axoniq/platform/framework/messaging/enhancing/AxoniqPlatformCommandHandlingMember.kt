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

package io.axoniq.platform.framework.messaging.enhancing

import org.axonframework.messaging.commandhandling.annotation.CommandHandlingMember

class AxoniqPlatformCommandHandlingMember<T: Any>(
        override val delegate: CommandHandlingMember<T>,
        declaringClassName: String
) : AxoniqPlatformMessageHandlingMember<T>(
        delegate,
        declaringClassName
), CommandHandlingMember<T> {

    override fun commandName(): String {
        return delegate.commandName()
    }

    override fun routingKey(): String {
        return delegate.routingKey()
    }

    override fun isFactoryHandler(): Boolean {
        return delegate.isFactoryHandler()
    }
}