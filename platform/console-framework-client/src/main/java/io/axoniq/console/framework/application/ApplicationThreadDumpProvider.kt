package io.axoniq.console.framework.application
import io.axoniq.console.framework.api.ThreadDumpEntry
import io.axoniq.console.framework.api.ThreadDumpResult
import java.lang.management.ManagementFactory

class ApplicationThreadDumpProvider() {
    private val threadBean = ManagementFactory.getThreadMXBean()

    fun collectThreadDumps(instanceName: String): ThreadDumpResult {
        val threadDumpEntries = threadBean.dumpAllThreads(true, true).map { threadInfo ->
            ThreadDumpEntry(
                    instanceName = instanceName,
                    threadName = threadInfo.threadName,
                    threadDump = threadInfo.toString()
            )
        }
        return ThreadDumpResult(timestamp = System.currentTimeMillis(), threadDumpEntries = threadDumpEntries)
    }
}