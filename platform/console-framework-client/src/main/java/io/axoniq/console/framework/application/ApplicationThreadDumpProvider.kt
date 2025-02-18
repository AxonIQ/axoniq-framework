package io.axoniq.console.framework.application
import io.axoniq.console.framework.api.ThreadDumpEntry
import io.axoniq.console.framework.api.ThreadDumpResult
import java.lang.management.ManagementFactory

class ApplicationThreadDumpProvider() {
    private val threadBean = ManagementFactory.getThreadMXBean()

    fun collectThreadDumps(instance: String): ThreadDumpResult {
        val threadDumpEntries = threadBean.dumpAllThreads(true, true).map { threadInfo ->
            ThreadDumpEntry(
                    instance = instance,
                    thread = threadInfo.threadName,
                    trace = threadInfo.toString()
            )
        }
        return ThreadDumpResult(timestamp = System.currentTimeMillis(), threadDumpEntries = threadDumpEntries)
    }
}