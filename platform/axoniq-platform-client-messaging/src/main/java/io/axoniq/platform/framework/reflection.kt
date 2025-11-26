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

package io.axoniq.platform.framework

import org.axonframework.common.ReflectionUtils


fun <T> Any.getPropertyValue(fieldName: String): T? {
    val field = ReflectionUtils.fieldsOf(this::class.java).firstOrNull { it.name == fieldName } ?: return null
    return ReflectionUtils.getMemberValue(
            field,
            this
    )
}

fun Any.getPropertyType(fieldName: String): String {
    return ReflectionUtils.getMemberValue<Any>(
            ReflectionUtils.fieldsOf(this::class.java).first { it.name == fieldName },
            this
    ).let { it::class.java.name }
}

fun Any.getPropertyType(fieldName: String, clazz: Class<out Any>): String {
    return ReflectionUtils.getMemberValue<Any>(
            ReflectionUtils.fieldsOf(this::class.java).first { it.name == fieldName },
            this
    )
            .let { it.unwrapPossiblyDecoratedClass(clazz) }
            .let { it::class.java.name }
}
