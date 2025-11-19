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

import org.axonframework.messaging.commandhandling.annotation.CommandHandlingMember

class AxoniqPlatformCommandHandlingMember<T>(
        override val delegate: CommandHandlingMember<T>,
        declaringClassName: String
) : AxoniqPlatformMessageHandlingMember<T>(
        delegate,
        declaringClassName
), CommandHandlingMember<T> {

    override fun commandName(): String? {
        return delegate.commandName()
    }

    override fun routingKey(): String? {
        return delegate.routingKey()
    }

    override fun isFactoryHandler(): Boolean {
        return delegate.isFactoryHandler()
    }
}