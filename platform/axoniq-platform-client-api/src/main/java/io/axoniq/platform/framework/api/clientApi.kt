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

package io.axoniq.platform.framework.api

@Deprecated("Use ClientSettingsV2 instead")
data class ClientSettings(
        val heartbeatInterval: Long,
        val heartbeatTimeout: Long,
        val processorReportInterval: Long,
        val handlerReportInterval: Long,
)

/**
 * Unfortunately the original [ClientSettings] was not backwards compatible :(
 * Unrecognized field "applicationReportInterval" (class io.axoniq.console.framework.api.ClientSettings), not marked as ignorable (4 known properties: "heartbeatInterval", "processorReportInterval", "heartbeatTimeout", "handlerReportInterval"])
 * Made a new version of the class, and will adjust future serializer versions to be lenient
 */
data class ClientSettingsV2(
        val heartbeatInterval: Long,
        val heartbeatTimeout: Long,
        val processorReportInterval: Long,
        val handlerReportInterval: Long,
        val applicationReportInterval: Long,
)