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

package io.axoniq.platform.framework.application

import java.lang.reflect.Method


fun Class<*>.detectMethod(bean: Any, name: String): Method? {
    return try {
        this.cast(bean)
        this.getMethod(name)
    } catch (e: Exception) {
        null
    }
}

fun List<String>.firstExistingClass(): Class<*>? {
    for (className in this) {
        try {
            return Class.forName(className)
        } catch (e: ClassNotFoundException) {
            // ignore
        }
    }
    return null
}