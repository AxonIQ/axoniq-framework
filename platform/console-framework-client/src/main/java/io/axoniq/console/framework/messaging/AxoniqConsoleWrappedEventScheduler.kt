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

package io.axoniq.console.framework.messaging

import io.axoniq.console.framework.api.AxoniqConsoleMessageOrigin
import io.axoniq.console.framework.api.metrics.DispatcherStatisticIdentifier
import io.axoniq.console.framework.api.metrics.HandlerStatisticsMetricIdentifier
import io.axoniq.console.framework.api.metrics.HandlerType
import io.axoniq.console.framework.api.metrics.MessageIdentifier
import org.axonframework.eventhandling.EventMessage
import org.axonframework.eventhandling.scheduling.EventScheduler
import org.axonframework.eventhandling.scheduling.ScheduleToken
import org.axonframework.lifecycle.Lifecycle
import org.axonframework.lifecycle.Lifecycle.LifecycleRegistry
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork
import java.time.Duration
import java.time.Instant

/**
 * Used to be able to report the Dispatcher for events send via a scheduler.
 * The original configured scheduler will become the delegate of this class.
 */
class AxoniqConsoleWrappedEventScheduler(
        private val delegate: EventScheduler,
        private val registry: HandlerMetricsRegistry,
        private val componentName: String,
) : EventScheduler, Lifecycle {
    override fun schedule(triggerDateTime: Instant, event: Any): ScheduleToken {
        registerEvent(event)
        return delegate.schedule(triggerDateTime, event)
    }

    override fun schedule(triggerDuration: Duration, event: Any): ScheduleToken {
        registerEvent(event)
        return delegate.schedule(triggerDuration, event)
    }

    override fun cancelSchedule(scheduleToken: ScheduleToken?) {
        delegate.cancelSchedule(scheduleToken)
    }

    override fun reschedule(scheduleToken: ScheduleToken, triggerDuration: Duration, event: Any): ScheduleToken {
        return delegate.reschedule(scheduleToken, triggerDuration, event)
    }

    override fun reschedule(scheduleToken: ScheduleToken, triggerDateTime: Instant, event: Any): ScheduleToken {
        return delegate.reschedule(scheduleToken, triggerDateTime, event)
    }

    override fun shutdown() {
        delegate.shutdown()
    }

    override fun registerLifecycleHandlers(lifecycleRegistry: LifecycleRegistry) {
        if (delegate is Lifecycle) {
            delegate.registerLifecycleHandlers(lifecycleRegistry)
        }
    }

    private fun registerEvent(event: Any) {
        if (!CurrentUnitOfWork.isStarted()) {
            // Determine the origin of the handler
            val origin = when {
                event is EventMessage<*> && event.payloadType?.javaClass?.isAnnotationPresent(AxoniqConsoleMessageOrigin::class.java) == true -> event.payloadType.getAnnotation(AxoniqConsoleMessageOrigin::class.java).name
                event.javaClass.isAnnotationPresent(AxoniqConsoleMessageOrigin::class.java) -> event.javaClass.getAnnotation(AxoniqConsoleMessageOrigin::class.java).name
                else -> componentName
            }
            reportMessageDispatchedFromOrigin(origin, event)
        } else {
            reportMessageDispatchedFromHandler(CurrentUnitOfWork.get().message.payload.javaClass.simpleName, event)
        }

    }

    private fun reportMessageDispatchedFromOrigin(originName: String, event: Any) {
        registry.registerMessageDispatchedDuringHandling(
                DispatcherStatisticIdentifier(HandlerStatisticsMetricIdentifier(
                        type = HandlerType.Origin,
                        component = originName,
                        message = MessageIdentifier("Dispatcher", originName)), event.toInformation())
        )
    }

    private fun reportMessageDispatchedFromHandler(handlerName: String, event: Any) {
        registry.registerMessageDispatchedDuringHandling(
                DispatcherStatisticIdentifier(HandlerStatisticsMetricIdentifier(
                        type = HandlerType.Message,
                        component = handlerName,
                        message = MessageIdentifier("EventMessage", handlerName)), event.toInformation())
        )
    }

    private fun Any.toInformation() = MessageIdentifier(
            EventMessage::class.java.simpleName,
            when (this) {
                is EventMessage<*> -> this.payloadType.name.toSimpleName()
                else -> this.javaClass.name.toSimpleName()
            }
    )
}