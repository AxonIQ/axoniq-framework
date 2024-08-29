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

import io.axoniq.console.framework.application.ApplicationMetricRegistry
import io.axoniq.console.framework.application.BusType
import io.axoniq.console.framework.application.MeasuringExecutorServiceDecorator
import io.axoniq.console.framework.messaging.SpanMatcherPredicateMap
import io.axoniq.console.framework.util.PostProcessHelper
import org.axonframework.commandhandling.CommandBus
import org.axonframework.common.ReflectionUtils
import org.axonframework.config.Configurer
import org.axonframework.config.ConfigurerModule
import org.axonframework.queryhandling.QueryBus
import java.lang.reflect.Field
import java.util.concurrent.ExecutorService

class AxoniqConsoleEnhancingConfigurerModule(private val spanMatcherPredicateMap: SpanMatcherPredicateMap) : ConfigurerModule {
    override fun configureModule(configurer: Configurer) {
        configurer.onInitialize { configuration ->
            configuration.onStart {
                enhance(configuration.spanFactory())
                enhance(configuration.eventStore())
                enhance(configuration.commandBus())
                val commandBus = configuration.commandBus().unwrapPossiblyDecoratedClass(CommandBus::class.java)
                enhanceExecutorService(commandBus, BusType.COMMAND, configuration.getComponent(ApplicationMetricRegistry::class.java))
                enhance(configuration.queryBus())
                val queryBus = configuration.queryBus().unwrapPossiblyDecoratedClass(QueryBus::class.java)
                enhanceExecutorService(queryBus, BusType.QUERY, configuration.getComponent(ApplicationMetricRegistry::class.java))
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

    private fun enhanceExecutorService(any: Any, query: BusType, registry: ApplicationMetricRegistry) {
        val field = getExecutorServiceField(any::class.java) ?: return
        val original = ReflectionUtils.getFieldValue(field, any) as ExecutorService
        val enhanced = MeasuringExecutorServiceDecorator(query, original, registry)
        ReflectionUtils.setFieldValue(field, any, enhanced)
    }

    private fun getExecutorServiceField(clazz: Class<*>): Field? {
        return ReflectionUtils.fieldsOf(clazz).firstOrNull { f ->
            f.type.isAssignableFrom(ExecutorService::class.java)
        }
    }
}
