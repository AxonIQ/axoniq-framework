/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
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

fun <T> Any.setPropertyValue(fieldName: String, value: T) {
    val field = ReflectionUtils.fieldsOf(this::class.java).firstOrNull { it.name == fieldName } ?: return
    field.isAccessible = true
    field.set(this, value)
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
