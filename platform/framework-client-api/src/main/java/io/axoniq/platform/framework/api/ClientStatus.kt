/*
 * Copyright (c) 2022-2026. AxonIQ B.V.
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


/**
 * Represents the status of the client.
 *
 * @param enabled whether the client is enabled to process reports and retrieve a license.
 */
enum class ClientStatus(val enabled: Boolean) {
    /**
     * The connection has just been established, but has not yet been validated by the server to be able to send reports
     * nor have a license. This is the default status of a connection, and will be changed to one of the other statuses after validation.
     */
    PENDING(false),

    /**
     * The connection has been accepted and this connection is part of the provisioned connections.
     * This means that the connection is able to process reports and retrieve a license, and does not use credits.
     */
    PROVISIONED(true),

    /**
     * The connection has been accepted, but the pool of provisioned connections is full.
     * This means that the connection is able to process reports and retrieve a license, but is using credits to do so.
     * Upon depletion of credits, the connection will be BLOCKED.
     * Can be changed to PROVISIONED when a provisioned connection is available.
     */
    USING_CREDITS(true),

    /**
     * The connection has been blocked, and is not able to process reports.
     * Once a provisioned connection opens up, or credits are available, the connection will be unblocked.
     */
    BLOCKED(false)
}