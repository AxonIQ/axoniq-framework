package io.axoniq.console.framework.api

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