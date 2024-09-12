/*
 * Copyright (c) 2022-2024. AxonIQ B.V.
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

package io.axoniq.console.framework.api

object Routes {

    object Management {
        // Route on both client and server. Client can pull settings whenever, server can push new settings
        const val SETTINGS = "client-settings"
        const val SETTINGS_V2 = "client-settings-v2"
        // Route on both client and server to receive/send heartbeats
        const val HEARTBEAT = "client-heartbeat"
        // Request to log something in the application
        const val LOG = "client-log"
        // Request to stop sending reports to the server in case a client goes over the limit and credits
        const val STOP_REPORTS = "client-reporting-stop"
        // Request to start sending reports again.
        const val START_REPORTS = "client-reporting-start"
    }

    object EventProcessor {
        const val REPORT = "processor-info-report"

        const val START = "processor-command-start"
        const val STOP = "processor-command-stop"

        const val RELEASE = "processor-command-release-segment"
        const val SPLIT = "processor-command-split-segment"
        const val MERGE = "processor-command-merge-segment"
        const val RESET = "processor-command-reset"

        const val STATUS = "event-processor-status"
        const val SEGMENTS = "event-processor-segments"
        const val CLAIM = "event-processor-claim"
    }

    object ProcessingGroup {

        object DeadLetter {
            const val LETTERS = "dlq-query-dead-letters"
            const val SEQUENCE_SIZE = "dlq-query-dead-letter-sequence-size"

            const val DELETE_SEQUENCE = "dlq-command-delete-sequence"
            const val DELETE_LETTER = "dlq-command-delete-letter"
            const val PROCESS = "dlq-command-process"
        }
    }

    object Application {
        const val REPORT = "application-info-report"
    }

    object MessageFlow {
        const val STATS = "message-flow-stats"
    }
}
