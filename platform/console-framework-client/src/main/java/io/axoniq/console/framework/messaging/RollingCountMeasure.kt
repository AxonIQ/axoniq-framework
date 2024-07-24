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

import java.util.concurrent.ConcurrentHashMap

const val BUCKET_SIZE = 1000L

/**
 * Keeps a statistic on the amount of messages received in the last 20 seconds, then tripled to
 * get the message rate per minute.
 */
class RollingCountMeasure {
    private val countMap = ConcurrentHashMap<Long, Int>()

    fun increment() {
        countMap.compute(currentBucket()) { _, v -> (v ?: 0) + 1 }
    }

    fun count(): Double {
        val bucket = currentBucket()
        val minimumBucket = (bucket - 11)
        val toRemove = countMap.keys.filter { it < minimumBucket }
        toRemove.forEach { countMap.remove(it) }

        return countMap.filter { it.key in minimumBucket until bucket }.values.sum().toDouble() * 6
    }

    private fun currentBucket(): Long {
        return System.currentTimeMillis() / BUCKET_SIZE
    }
}
