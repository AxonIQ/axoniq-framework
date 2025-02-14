package io.axoniq.console.framework.application

import io.axoniq.console.framework.api.*
import io.axoniq.console.framework.client.RSocketHandlerRegistrar
import org.axonframework.lifecycle.Lifecycle
import org.axonframework.lifecycle.Phase
import org.slf4j.LoggerFactory

open class RSocketThreadDumpResponder (
        private val applicationThreadDumpProvider: ApplicationThreadDumpProvider,
        private val registrar: RSocketHandlerRegistrar
) : Lifecycle {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun registerLifecycleHandlers(registry: Lifecycle.LifecycleRegistry) {
        registry.onStart(Phase.EXTERNAL_CONNECTIONS, this::start)
    }

    fun start() {
        registrar.registerHandlerWithPayload(
                Routes.Management.THREAD_DUMP,
                ThreadDumpQuery::class.java,
                this::handleThreadDumpQuery
        )
    }

    private fun handleThreadDumpQuery(query: ThreadDumpQuery): ThreadDumpResult {
        logger.debug("Handling AxonIQ Console THREAD_DUMP query for request [{}]", query)
        return applicationThreadDumpProvider.collectThreadDumps(query.instanceName)
    }
}