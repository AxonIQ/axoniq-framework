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

import io.axoniq.platform.framework.api.metrics.MessageIdentifier
import io.axoniq.platform.framework.api.metrics.StatisticDistribution
import io.micrometer.core.instrument.distribution.HistogramSnapshot
import io.micrometer.core.instrument.distribution.ValueAtPercentile
import org.axonframework.messaging.commandhandling.CommandMessage
import org.axonframework.messaging.core.Message
import org.axonframework.messaging.eventhandling.EventMessage
import org.axonframework.messaging.queryhandling.QueryMessage
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage
import java.util.concurrent.TimeUnit


fun Message.toInformation() = MessageIdentifier(
        when (this) {
            is CommandMessage -> CommandMessage::class.java.simpleName
            is EventMessage -> EventMessage::class.java.simpleName
            is QueryMessage -> QueryMessage::class.java.simpleName
            is SubscriptionQueryUpdateMessage -> SubscriptionQueryUpdateMessage::class.java.simpleName
            else -> this::class.java.simpleName
        },
        this.type().name()
)


fun HistogramSnapshot.toDistribution(): StatisticDistribution {
    val percentiles = percentileValues()
    return StatisticDistribution(
            min = percentiles.ofPercentile(0.01),
            percentile90 = percentiles.ofPercentile(0.90),
            percentile95 = percentiles.ofPercentile(0.95),
            median = percentiles.ofPercentile(0.50),
            mean = mean(TimeUnit.MILLISECONDS),
            max = percentiles.ofPercentile(1.00),
    )
}

fun Array<ValueAtPercentile>.ofPercentile(percentile: Double): Double {
    return this.firstOrNull { pc -> pc.percentile() == percentile }
            ?.value(TimeUnit.MILLISECONDS)!!
}
