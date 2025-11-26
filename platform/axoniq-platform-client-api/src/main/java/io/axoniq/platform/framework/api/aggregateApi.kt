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

package io.axoniq.platform.framework.api

import java.time.Instant

data class DomainEventsResult(
        val entityId: String,
        val entityType: String,
        val domainEvents: List<DomainEvent>,
        val page: Int,
        val pageSize: Int,
        val totalCount: Long,
)

data class DomainEvent(
        val sequenceNumber: Long,
        val timestamp: Instant,
        val payloadType: String,
        val payload: String?
)

data class DomainEventsQuery(
        val entityId: String,
        val page: Int = 0,
        val pageSize: Int = 10,
)

data class EntityStateResult(
    val type: String,
    val entityId: String,
    val maxSequenceNumber: Long = 0,
    val state: String?,
)

data class EntityStateAtSequenceQuery(
        val type: String,
        val entityId: String,
        val maxSequenceNumber: Long = 0,
)