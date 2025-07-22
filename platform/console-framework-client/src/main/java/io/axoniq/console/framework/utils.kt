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

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.axonframework.common.ReflectionUtils
import java.lang.reflect.Field
import java.time.Duration

/**
 * Find fields matching its own type. If found, it will unwrap the deeper value.
 * Useful for when users wrap components as decorators, like Axon FireStarter does.
 */
fun <T : Any> T.unwrapPossiblyDecoratedClass(clazz: Class<out T>): T {
    return fieldsOfMatchingType(clazz)
            .mapNotNull { ReflectionUtils.getFieldValue(it, this) as T? }
            .map { it.unwrapPossiblyDecoratedClass(clazz) }
            .firstOrNull()
            // No field of provided type - reached end of decorator chain
            ?: this
}

private fun <T : Any> T.fieldsOfMatchingType(clazz: Class<out T>): List<Field> {
    // When we reach our own AS-classes, stop unwrapping
    if (this::class.java.name.startsWith("org.axonframework") && this::class.java.simpleName.startsWith("AxonServer")) return listOf()
    return ReflectionUtils.fieldsOf(this::class.java)
            .filter { f -> clazz.isAssignableFrom(f.type) }
}

fun <K, V> MutableMap<K, V>.computeIfAbsentWithRetry(key: K, retries: Int = 0, defaultValue: (K) -> V): V {
    try {
        return computeIfAbsent(key, defaultValue)
    } catch (e: ConcurrentModificationException) {
        if(retries < 3) {
            return computeIfAbsentWithRetry(key, retries + 1, defaultValue)
        }
        // We cannot get it from the map. Return the default value without putting it in, so the code can continue.
        return defaultValue(key)
    }
}

fun createTimer(meterRegistry: MeterRegistry, name: String): Timer {
    return Timer
            .builder(name)
            .publishPercentiles(1.00, 0.95, 0.90, 0.50, 0.01)
            .distributionStatisticExpiry(Duration.ofMinutes(5))
            .distributionStatisticBufferLength(5)
            .register(meterRegistry)
}

/**
 * Truncates the string to ensure it doesn't exceed the specified maximum byte size.
 *
 * If the string's byte representation (UTF-8 encoding) is larger than [maxBytes],
 * it will be truncated and a truncation message will be appended. The final result
 * (including the truncation message) will not exceed [maxBytes] when encoded as UTF-8.
 *
 * @param maxBytes The maximum allowed byte size for the final string (including truncation message).
 *                 Must be at least 16 bytes to accommodate the truncation message.
 * @return The original string if it's within the byte limit, null if input is null,
 *         or a truncated string with truncation message if it exceeds the byte limit.
 * @throws IllegalArgumentException if [maxBytes] is too small to accommodate the truncation message.
 */
fun String?.truncateToBytes(maxBytes: Int): String? {
    if (this == null) return null

    val originalBytes = this.toByteArray(Charsets.UTF_8)
    if (originalBytes.size <= maxBytes) {
        return this
    }

    val truncationMessage = "... [truncated]"
    val messageBytes = truncationMessage.toByteArray(Charsets.UTF_8)

    if (maxBytes < messageBytes.size) {
        throw IllegalArgumentException(
                "maxBytes ($maxBytes) must be at least ${messageBytes.size} bytes to accommodate truncation message"
        )
    }

    val contentBytesLimit = maxBytes - messageBytes.size

    // Safely truncate at character boundary to avoid invalid UTF-8
    var truncatedContent = ""
    var currentBytes = 0

    for (char in this) {
        val charBytes = char.toString().toByteArray(Charsets.UTF_8).size
        if (currentBytes + charBytes > contentBytesLimit) {
            break
        }
        truncatedContent += char
        currentBytes += charBytes
    }

    return truncatedContent + truncationMessage
}