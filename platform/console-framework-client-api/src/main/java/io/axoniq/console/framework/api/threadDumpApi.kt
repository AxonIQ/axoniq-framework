package io.axoniq.console.framework.api

data class ThreadDumpEntry(
        val instanceName: String,
        val threadName: String,
        val threadDump: String,
)

data class ThreadDumpResult(
        val timestamp: Long,
        val threadDumpEntries: List<ThreadDumpEntry>
)

data class ThreadDumpQuery(
        val instanceName: String
)