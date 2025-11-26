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