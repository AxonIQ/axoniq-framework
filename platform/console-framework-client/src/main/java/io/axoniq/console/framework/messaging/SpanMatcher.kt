/*
 * Copyright (c) 2022-2023. AxonIQ B.V.
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
package io.axoniq.console.framework.messaging

import org.axonframework.util.MavenArtifactVersionResolver
import java.util.*
import java.util.function.Predicate

/**
 * Enum used by the {@link AxoniqConsoleSpanFactory} to determine if it should call certain methods, bases on the span
 * name. This makes it possible to support the way spans are implemented by default for version 4.6 to 4.8 as well as
 * the version from 4.9. It also makes it possible to accommodate for custom span implementations.
 */
enum class SpanMatcher(val pre49Predicate: Predicate<String>, val from49Predicate: Predicate<String>) {
    HANDLER(
            Predicate { name: String ->
                name == "QueryProcessingTask" ||
                        name == "AxonServerCommandBus.handle" ||
                        name == "DeadlineJob.execute"
            },
            Predicate { name: String ->
                name == "QueryBus.processQueryMessage" ||
                        name == "CommandBus.handleCommand" ||
                        name.startsWith("DeadlineManager.executeDeadline(")
            }),
    OBTAIN_LOCK(
            Predicate { name: String -> name == "LockingRepository.obtainLock" },
            Predicate { name: String -> name == "Repository.obtainLock" }),
    LOAD(
            Predicate { name: String -> name.contains(".load ") },
            Predicate { name: String -> name == "Repository.load" }),
    COMMIT(
            Predicate { name: String -> name.endsWith(".commit") },
            Predicate { name: String -> name == "EventBus.commitEvents" }),
    SUBCRIBING_EVENT_HANDLER(
            Predicate { name: String -> name.startsWith("SubscribingEventProcessor[") && name.endsWith(".process")},
            Predicate { name: String -> name == "EventProcessor.process" }),
    MESSAGE_START(
            Predicate { name: String ->
                name.endsWith("Bus.handle") ||
                        name == "SimpleQueryBus.query" ||
                        name.startsWith("SimpleQueryBus.scatterGather") ||
                        name.startsWith("PooledStreamingEventProcessor") ||
                        name.startsWith("TrackingEventProcessor") ||
                        name.endsWith("].startNewSaga") ||
                        name.contains("].invokeSaga ")
            },
            Predicate { name: String ->
                name == "CommandBus.dispatchCommand" ||
                        name == "QueryBus.query" ||
                        name.startsWith("QueryBus.scatterGatherQuery") ||
                        name.startsWith("StreamingEventProcessor") ||
                        name.startsWith("SagaManager.createSaga(") ||
                        name.startsWith("SagaManager.invokeSaga(")
            });

    companion object {

        val pre49Version: Boolean by lazy { pre49Version() }
        private fun pre49PredicateMap(): SpanMatcherPredicateMap {
            val spanMatcherPredicateMap = SpanMatcherPredicateMap()
            SpanMatcher.values().forEach { s -> spanMatcherPredicateMap[s] = s.pre49Predicate }
            return spanMatcherPredicateMap
        }

        private fun from49PredicateMap(): SpanMatcherPredicateMap {
            val spanMatcherPredicateMap = SpanMatcherPredicateMap()
            SpanMatcher.values().forEach { s -> spanMatcherPredicateMap[s] = s.from49Predicate }
            return spanMatcherPredicateMap
        }

        private fun pre49Version(): Boolean {
            val version = MavenArtifactVersionResolver(
                    "org.axonframework",
                    "axon-messaging",
                    SpanMatcher::class.java.classLoader
            ).get() ?: "Unknown"
            return version.startsWith("4.6") || version.startsWith("4.7") || version.startsWith("4.8")
        }

        /**
         * Get the mapping for the spans, based on the version of axon-messaging.
         */
        @JvmStatic
        fun getSpanMatcherPredicateMap(): SpanMatcherPredicateMap =
                if (pre49Version)
                    pre49PredicateMap()
                else
                    from49PredicateMap()
    }
}

class SpanMatcherPredicateMap : EnumMap<SpanMatcher, Predicate<String>>(SpanMatcher::class.java)


