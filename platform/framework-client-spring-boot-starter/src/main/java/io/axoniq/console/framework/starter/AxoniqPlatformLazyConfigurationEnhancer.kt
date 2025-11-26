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

package io.axoniq.console.framework.starter

import io.axoniq.platform.framework.eventprocessor.AxoniqPlatformEventHandlingComponent
import io.axoniq.platform.framework.eventprocessor.ProcessorMetricsRegistry
import io.axoniq.platform.framework.getPropertyValue
import io.axoniq.platform.framework.messaging.HandlerMetricsRegistry
import org.axonframework.common.configuration.*
import org.axonframework.extension.spring.config.SpringLazyCreatingModule
import org.axonframework.messaging.eventhandling.EventHandlingComponent
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorModule
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessorModule
import java.util.function.Function


/**
 * Spring might implement some modules lazily
 */
class AxoniqPlatformLazyConfigurationEnhancer : ConfigurationEnhancer {
    override fun enhance(registry: ComponentRegistry) {
        registry.doOnLazySubmodules { componentRegistry: ComponentRegistry?, module: Module? ->
            componentRegistry!!
                    .registerDecorator(DecoratorDefinition.forType(EventHandlingComponent::class.java)
                            .with { cc: Configuration?, name: String?, delegate: EventHandlingComponent? ->
                                AxoniqPlatformEventHandlingComponent(
                                        delegate!!,
                                        getProcessorName(module),
                                        cc!!.getComponent(HandlerMetricsRegistry::class.java),
                                        cc.getComponent(ProcessorMetricsRegistry::class.java))
                            }
                            .order(0))
        }
    }

    private fun getProcessorName(module: Module?): String {
        var processorName: String? = null
        if (module is PooledStreamingEventProcessorModule) {
            processorName = module.getPropertyValue("processorName")
        } else if (module is SubscribingEventProcessorModule) {
            processorName = module.getPropertyValue("processorName")
        }
        return processorName ?: "UnknownProcessor"
    }

    fun ComponentRegistry.doOnLazySubmodules(block: (ComponentRegistry, Module?) -> Unit) {
        val modules = this.getPropertyValue<MutableMap<String, Module>>("modules")
        modules?.forEach { entry ->
            val module = entry.value
            if (module is SpringLazyCreatingModule<*>) {
                // We need to wrap the module with our own actions so the module is enhanced when created
                val constructor = SpringLazyCreatingModule::class.java.getDeclaredConstructor(String::class.java, Function::class.java)
                val wrappedFunction: Function<Configuration, ModuleBuilder<out Module>> = Function { config ->
                    val originalBuilderFunction = module.getPropertyValue<Function<Configuration, ModuleBuilder<out Module>>>("moduleBuilder")
                    ModuleBuilder {
                        val originalBuilder = originalBuilderFunction!!.apply(config)
                        block(originalBuilder.getPropertyValue<ComponentRegistry>("componentRegistry")!!, originalBuilder as Module)
                        val builtModule = originalBuilder.build()
                        builtModule
                    }
                }
                constructor.isAccessible = true
                val moduleToReplace = constructor.newInstance(module.name(), wrappedFunction)
                modules[entry.key] = moduleToReplace
            }
            module.getPropertyValue<ComponentRegistry>("componentRegistry")?.let { cr ->
                block(cr, module)
                cr.doOnLazySubmodules(block)
            }
        }
    }
}