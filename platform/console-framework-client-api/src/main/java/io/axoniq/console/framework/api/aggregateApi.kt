package io.axoniq.console.framework.api

import java.time.Instant

data class DomainEventsResult(
        val aggregateId: String,
        val aggregateType: String,
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
        val aggregateId: String,
        val page: Int = 0,
        val pageSize: Int = 10,
)

data class AggregateSnapshotResult(
        val type: String,
        val aggregateId: String,
        val maxSequenceNumber: Long = 0,
        val snapshot: String,
)

data class AggregateSnapshotQuery(
        val type: String,
        val aggregateId: String,
        val maxSequenceNumber: Long = 0,
)