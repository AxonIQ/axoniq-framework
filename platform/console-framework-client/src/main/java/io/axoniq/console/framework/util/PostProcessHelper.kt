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

package io.axoniq.console.framework.util

import io.axoniq.console.framework.messaging.AxoniqConsoleSpanFactory
import io.axoniq.console.framework.messaging.SpanMatcher
import io.axoniq.console.framework.messaging.SpanMatcherPredicateMap
import io.axoniq.console.framework.unwrapPossiblyDecoratedClass
import org.axonframework.commandhandling.CommandBus
import org.axonframework.common.ReflectionUtils
import org.axonframework.deadline.DeadlineManager
import org.axonframework.eventhandling.EventHandlerInvoker
import org.axonframework.eventhandling.EventProcessor
import org.axonframework.eventsourcing.Snapshotter
import org.axonframework.eventsourcing.eventstore.EventStore
import org.axonframework.queryhandling.QueryBus
import org.axonframework.queryhandling.QueryUpdateEmitter
import org.axonframework.tracing.MultiSpanFactory
import org.axonframework.tracing.NoOpSpanFactory
import org.axonframework.tracing.SpanFactory
import org.slf4j.LoggerFactory
import java.lang.reflect.Field


class PostProcessHelper {
    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
        fun enhance(bean: Any, beanName: String, spanMatcherPredicateMap: SpanMatcherPredicateMap): Any {
            return when (bean) {
                is SpanFactory -> bean.enhance(spanMatcherPredicateMap)
                is EventStore -> bean.enhanceWithin(beanName, spanMatcherPredicateMap, EventStore::class.java)
                is CommandBus -> bean.enhanceWithin(beanName, spanMatcherPredicateMap, CommandBus::class.java)
                is QueryBus -> bean.enhanceWithin(beanName, spanMatcherPredicateMap, QueryBus::class.java)
                is QueryUpdateEmitter -> bean.enhanceWithin(beanName, spanMatcherPredicateMap, QueryUpdateEmitter::class.java)
                is EventProcessor -> bean.enhanceWithin(beanName, spanMatcherPredicateMap, EventProcessor::class.java)
                is Snapshotter -> bean.enhanceWithin(beanName, spanMatcherPredicateMap, Snapshotter::class.java)
                is DeadlineManager -> bean.enhanceWithin(beanName, spanMatcherPredicateMap, DeadlineManager::class.java)
                is EventHandlerInvoker -> bean.enhanceWithin(beanName, spanMatcherPredicateMap, EventHandlerInvoker::class.java)
                else -> bean
            }
        }

        private fun MultiSpanFactory.spanFactories(): List<SpanFactory> {
            val field = ReflectionUtils.fieldsOf(MultiSpanFactory::class.java).first { f -> f.name == "spanFactories" }
            return ReflectionUtils.getFieldValue(field, this)
        }

        private fun SpanFactory.needsEnhancement(): Boolean {
            if (this is AxoniqConsoleSpanFactory) {
                return false
            }
            if (this is MultiSpanFactory) {
                return this.spanFactories().all { it.needsEnhancement() }
            }
            return true
        }

        private fun SpanFactory.enhance(spanMatcherPredicateMap: SpanMatcherPredicateMap): SpanFactory {
            if (!needsEnhancement()) {
                return this
            }
            val spanFactory = AxoniqConsoleSpanFactory(spanMatcherPredicateMap)
            return when (this) {
                is NoOpSpanFactory -> {
                    spanFactory
                }

                is MultiSpanFactory -> {
                    MultiSpanFactory(this.spanFactories() + spanFactory)
                }

                else -> MultiSpanFactory(
                        listOf(
                                spanFactory,
                                this
                        )
                )
            }
        }

        private fun getSpanFactoryField(clazz: Class<Any>): Field? {
            return ReflectionUtils.fieldsOf(clazz).firstOrNull { f ->
                f.name == "spanFactory" || f.type.isAssignableFrom(SpanFactory::class.java)
            }
        }


        private fun getSpanFactory(bean: Any): Any? {
            return getSpanFactoryField(bean.javaClass)?.let { ReflectionUtils.getFieldValue(it, bean) }
        }

        private fun setSpanFactory(bean: Any, spanFactory: SpanFactory) {
            getSpanFactoryField(bean.javaClass)?.let {
                ReflectionUtils.setFieldValue(it, bean, spanFactory)
            }
        }

        /**
         * For versions after 4.9 we need to ensure we enhance a wrapped span factory, else we might enhance one of the
         * intermediate span factory classes introduced with 4.9 for a generic span factory, causing problems at runtime
         * because it doesn't have some of the methods.
         */
        private fun enhanceWithin(bean: Any, spanMatcherPredicateMap: SpanMatcherPredicateMap) {
            getSpanFactory(bean)?.let {
                if (SpanMatcher.pre49Version && it is SpanFactory && it.needsEnhancement()) {
                    setSpanFactory(bean, it.enhance(spanMatcherPredicateMap))
                } else {
                    getSpanFactory(it)?.let { inner ->
                        if (inner is SpanFactory && inner.needsEnhancement()) {
                            setSpanFactory(it, inner.enhance(spanMatcherPredicateMap))
                        }
                    }
                }
            }
        }

        private fun <T : Any> T.enhanceWithin(
                beanName: String,
                spanMatcherPredicateMap: SpanMatcherPredicateMap, clazz: Class<T>
        ): T {
            runCatching {
                val inner = this.unwrapPossiblyDecoratedClass(clazz)
                enhanceWithin(inner, spanMatcherPredicateMap)
            }.onFailure {
                logger.warn("Was unable to enhance bean with name: $beanName of class: ${this.javaClass}." +
                        "\nFor AxonIQ console you need to ensure the SpanFactory is set from the Spring Context.", it)
            }
            return this
        }
    }
}