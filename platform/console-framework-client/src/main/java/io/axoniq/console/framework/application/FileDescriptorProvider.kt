/*
 * Copyright (c) 2024. AxonIQ B.V.
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

package io.axoniq.console.framework.application

import java.lang.management.ManagementFactory

/**
 * Provides file metrics.
 * Inspired by micrometer code, but adjusted to work well for us
 */
class FileDescriptorProvider {
    private val osBean = ManagementFactory.getOperatingSystemMXBean()
    private val osBeanClass = listOf("com.sun.management.UnixOperatingSystemMXBean", "com.ibm.lang.management.UnixOperatingSystemMXBean").firstExistingClass()

    private val openMethod = osBeanClass?.detectMethod(osBean, "getOpenFileDescriptorCount")
    private val maxMethod = osBeanClass?.detectMethod(osBean, "getMaxFileDescriptorCount")

    fun getOpenFileDescriptors(): Long {
        return openMethod?.invoke(osBean) as Long? ?: -1
    }

    fun getMaxOpenFileDescriptors(): Long {
        return maxMethod?.invoke(osBean) as Long? ?: -1
    }
}