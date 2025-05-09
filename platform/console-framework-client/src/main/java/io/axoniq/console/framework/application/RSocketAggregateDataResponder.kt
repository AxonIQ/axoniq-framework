package io.axoniq.console.framework.application

import io.axoniq.console.framework.DomainEventAccessMode
import io.axoniq.console.framework.api.*
import io.axoniq.console.framework.client.RSocketHandlerRegistrar
import org.axonframework.lifecycle.Lifecycle
import org.axonframework.lifecycle.Phase
import org.axonframework.serialization.Serializer
import org.slf4j.LoggerFactory

open class RSocketAggregateDataResponder(
        private val aggregateEventStreamProvider: AggregateEventStreamProvider,
        private val registrar: RSocketHandlerRegistrar,
        private val domainEventAccessMode: DomainEventAccessMode,
        private val serializer: Serializer,
) : Lifecycle {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun registerLifecycleHandlers(registry: Lifecycle.LifecycleRegistry) {
        registry.onStart(Phase.EXTERNAL_CONNECTIONS, this::start)
    }

    fun start() {
        registrar.registerHandlerWithPayload(
                Routes.Aggregate.DOMAIN_EVENTS,
                DomainEventsQuery::class.java,
                this::handleDomainEventsQuery
        )
        registrar.registerHandlerWithPayload(
                Routes.Aggregate.LOAD_FOR_AGGREGATE,
                AggregateSnapshotQuery::class.java,
                this::handleLoadForAggregateQuery
        )
    }

    private fun handleDomainEventsQuery(query: DomainEventsQuery): DomainEventsResult {
        logger.debug("Handling AxonIQ Console DOMAIN_EVENTS query for request [{}]", query)
        logger.debug("Access mode is set on: {}.", domainEventAccessMode)

        val events = aggregateEventStreamProvider.getDomainEventStream(query.aggregateId)
                ?: throw IllegalArgumentException("No events found for aggregateId: ${query.aggregateId}")

        val includePayload = domainEventAccessMode == DomainEventAccessMode.FULL ||
                domainEventAccessMode == DomainEventAccessMode.PREVIEW_PAYLOAD_ONLY

        val totalEvents = events.size.toLong()
        val startIndex = query.page * query.pageSize

        if (startIndex >= totalEvents) {
            val fallbackEvent = events.minByOrNull { it.sequenceNumber }
            return DomainEventsResult(
                    aggregateId = fallbackEvent?.aggregateIdentifier ?: query.aggregateId,
                    aggregateType = fallbackEvent?.type ?: "",
                    domainEvents = emptyList(),
                    page = query.page,
                    pageSize = query.pageSize,
                    totalCount = totalEvents
            )
        }

        val pagedEvents = events.asSequence()
                .sortedBy { it.sequenceNumber }
                .drop(startIndex)
                .take(query.pageSize)
                .map { event ->
                    DomainEvent(
                            sequenceNumber = event.sequenceNumber,
                            timestamp = event.timestamp,
                            payloadType = event.payloadType.toString(),
                            payload = if (includePayload)
                                serializer.serialize(event.payload, String::class.java).data else ""
                    )
                }.toList()

        val firstEvent = events.minByOrNull { it.sequenceNumber }

        return DomainEventsResult(
                aggregateId = firstEvent?.aggregateIdentifier ?: query.aggregateId,
                aggregateType = firstEvent?.type ?: "",
                domainEvents = pagedEvents,
                page = query.page,
                pageSize = query.pageSize,
                totalCount = totalEvents
        )
    }


    private fun handleLoadForAggregateQuery(query: AggregateSnapshotQuery): AggregateSnapshotResult {
        logger.debug("Handling AxonIQ Console LOAD_FOR_AGGREGATE query for request [{}]", query)
        takeIf {
            logger.debug("Access mode is set on: {}.", domainEventAccessMode)
            domainEventAccessMode == DomainEventAccessMode.FULL ||
                    domainEventAccessMode == DomainEventAccessMode.LOAD_SNAPSHOT_ONLY
        }?.let {
            aggregateEventStreamProvider.loadForAggregate<String>(
                    type = query.type,
                    identifier = query.aggregateId,
                    maxSequenceNumber = query.maxSequenceNumber)?.let {
                return AggregateSnapshotResult(
                        type = query.type,
                        aggregateId = query.aggregateId,
                        maxSequenceNumber = query.maxSequenceNumber,
                        snapshot = it
                )
            } ?: throw IllegalArgumentException("Could not load snapshot for aggregateId: ${query.aggregateId}")
        } ?: throw IllegalArgumentException("Access mode is set on: $domainEventAccessMode. Cannot load the aggregate.")
    }
}