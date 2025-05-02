package io.axoniq.console.framework.application

import io.axoniq.console.framework.DomainEventAccessMode
import io.axoniq.console.framework.api.*
import io.axoniq.console.framework.client.RSocketHandlerRegistrar
import org.axonframework.lifecycle.Lifecycle
import org.axonframework.lifecycle.Phase
import org.slf4j.LoggerFactory

open class RSocketAggregateDataResponder(
        private val aggregateEventStreamProvider: AggregateEventStreamProvider,
        private val registrar: RSocketHandlerRegistrar,
        private val domainEventAccessMode: DomainEventAccessMode,
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

        return DomainEventsResult(
                aggregateId = events.first().aggregateIdentifier,
                aggregateType = events.first().type,
                domainEvents = events.map { event ->
                    DomainEvent(
                            sequenceNumber = event.sequenceNumber,
                            timestamp = event.timestamp,
                            payloadType = event.payloadType.toString(),
                            payload = if (includePayload) event.payload.toString() else ""
                    )
                }.sortedBy { it.sequenceNumber }
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
                        snapshot = it.toString()
                )
            } ?: throw IllegalArgumentException("Could not load snapshot for aggregateId: ${query.aggregateId}")
        } ?: throw IllegalArgumentException("Access mode is set on: $domainEventAccessMode. Cannot load the aggregate.")
    }
}