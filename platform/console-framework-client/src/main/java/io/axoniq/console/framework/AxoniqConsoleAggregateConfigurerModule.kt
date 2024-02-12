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

import io.axoniq.console.framework.messaging.AxoniqConsoleWrappedEventStore
import org.axonframework.common.ReflectionUtils
import org.axonframework.config.AggregateConfiguration
import org.axonframework.config.Configurer
import org.axonframework.config.ConfigurerModule
import org.axonframework.eventsourcing.EventSourcingRepository
import org.axonframework.eventsourcing.eventstore.EventStore
import org.axonframework.modelling.command.Repository
import kotlin.reflect.full.superclasses

class AxoniqConsoleAggregateConfigurerModule : ConfigurerModule {
    override fun configureModule(configurer: Configurer) {
        configurer.onInitialize {
            it.onStart {
                it.findModules(AggregateConfiguration::class.java).forEach { ac ->
                    val repo = ac.repository().unwrapPossiblyDecoratedClass(Repository::class.java)
                    if (repo is EventSourcingRepository) {
                        wrapIfPresentAndNotWrapped(repo, repo::class.java)
                        repo::class.superclasses.forEach { superClass ->
                            wrapIfPresentAndNotWrapped(repo, superClass.java)
                        }
                    }
                }
            }
        }
    }

    private fun wrapIfPresentAndNotWrapped(repo: EventSourcingRepository<*>, clazz: Class<*>) {
        val field =
                ReflectionUtils.fieldsOf(clazz).firstOrNull { f -> f.name == "eventStore" }
        if (field != null) {
            val current = ReflectionUtils.getFieldValue<EventStore>(field, repo)
            if (current !is AxoniqConsoleWrappedEventStore) {
                ReflectionUtils.setFieldValue(field, repo, AxoniqConsoleWrappedEventStore(current))
            }
        }
    }
}
