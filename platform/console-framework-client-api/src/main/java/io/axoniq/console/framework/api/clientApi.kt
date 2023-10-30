package io.axoniq.console.framework.api

data class ClientSettings(
        val heartbeatInterval: Long,
        val heartbeatTimeout: Long,
        val processorReportInterval: Long,
        val handlerReportInterval: Long,
)