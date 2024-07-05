/*
 * Copyright (c) 2022-2024. AxonIQ B.V.
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

package io.axoniq.console.framework

import io.axoniq.console.framework.messaging.SpanMatcherPredicateMap
import io.axoniq.console.framework.util.PostProcessHelper
import org.axonframework.config.Configurer
import org.axonframework.config.ConfigurerModule

class AxoniqConsoleEnhancingConfigurerModule(private val spanMatcherPredicateMap: SpanMatcherPredicateMap) : ConfigurerModule {
    override fun configureModule(configurer: Configurer) {
        configurer.onInitialize { configuration ->
            configuration.onStart {
                enhance(configuration.eventStore())
                enhance(configuration.commandBus())
                enhance(configuration.queryBus())
                enhance(configuration.queryUpdateEmitter())
                enhance(configuration.snapshotter())
                enhance(configuration.deadlineManager())
                configuration.eventProcessingConfiguration().sagaConfigurations().forEach { enhance(it.manager()) }
                configuration.eventProcessingConfiguration().eventProcessors().values.forEach { enhance(it) }
            }
        }
    }

    private fun enhance(any: Any) {
        PostProcessHelper.enhance(any, any::class.java.simpleName, spanMatcherPredicateMap)
    }
}
