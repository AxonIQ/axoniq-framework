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

import io.axoniq.platform.framework.api.Routes
import io.axoniq.platform.framework.api.ThreadDumpQuery
import io.axoniq.platform.framework.api.ThreadDumpResult
import io.axoniq.platform.framework.client.RSocketHandlerRegistrar
import org.slf4j.LoggerFactory

open class RSocketThreadDumpResponder(
        private val applicationThreadDumpProvider: ApplicationThreadDumpProvider,
        private val registrar: RSocketHandlerRegistrar
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    fun start() {
        registrar.registerHandlerWithPayload(
                Routes.Management.THREAD_DUMP,
                ThreadDumpQuery::class.java,
                this::handleThreadDumpQuery
        )
    }

    private fun handleThreadDumpQuery(query: ThreadDumpQuery): ThreadDumpResult {
        logger.debug("Handling Axoniq Platform  THREAD_DUMP query for request [{}]", query)
        return applicationThreadDumpProvider.collectThreadDumps(query.instance)
    }
}