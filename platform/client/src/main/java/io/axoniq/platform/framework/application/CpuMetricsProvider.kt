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

import java.lang.management.ManagementFactory

/**
 * Provides CPU metrics for the system and the process.
 * Inspired by micrometer code, but adjusted to work well for us
 */
class CpuMetricsProvider {
    private val osBean = ManagementFactory.getOperatingSystemMXBean()
    private val osBeanClass = listOf(
            "com.ibm.lang.management.OperatingSystemMXBean", "com.sun.management.OperatingSystemMXBean"
    ).firstExistingClass()

    private val cpuLoadMethod = osBeanClass?.detectMethod(osBean, "getCpuLoad") ?: osBeanClass?.detectMethod(osBean, "getSystemCpuLoad")
    private val processCpuUsageMethod = osBeanClass?.detectMethod(osBean, "getProcessCpuLoad")

    fun getSystemCpuUsage(): Double {
        return cpuLoadMethod?.invoke(osBean) as Double? ?: -1.0
    }

    fun getProcessCpuUsage(): Double {
        return processCpuUsageMethod?.invoke(osBean) as Double? ?: -1.0
    }
}