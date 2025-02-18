package io.axoniq.console.framework.api

data class ThreadDumpEntry(
        val instance: String,
        val thread: String,
        val trace: String,
)

data class ThreadDumpResult(
        val timestamp: Long,
        val threadDumpEntries: List<ThreadDumpEntry>
)

data class ThreadDumpQuery(
        val instance: String
)