package io.axoniq.console.framework.application

import io.axoniq.console.framework.api.*
import io.axoniq.console.framework.client.RSocketHandlerRegistrar
import org.axonframework.lifecycle.Lifecycle
import org.axonframework.lifecycle.Phase
import org.axonframework.serialization.Serializer
import org.slf4j.LoggerFactory

open class RSocketDomainEntityDataResponder(
        private val domainEventStreamProvider: DomainEventStreamProvider,
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
                Routes.Enity.DOMAIN_EVENTS,
                DomainEventsQuery::class.java,
                this::handleDomainEventsQuery
        )
        registrar.registerHandlerWithPayload(
                Routes.Enity.ENTITY_STATE_AT_SEQUENCE,
                EntityStateAtSequenceQuery::class.java,
                this::handleEntityStateAtSequenceQuery
        )
    }

    private fun handleDomainEventsQuery(query: DomainEventsQuery): DomainEventsResult {
        logger.debug("Handling AxonIQ Console DOMAIN_EVENTS query for request [{}]", query)
        logger.debug("Access mode is set on: {}.", domainEventAccessMode)

        val startIndex = query.page * query.pageSize

        // Determine whether to include payload based on the access mode
        val includePayload = domainEventAccessMode == DomainEventAccessMode.FULL ||
                domainEventAccessMode == DomainEventAccessMode.PREVIEW_PAYLOAD_ONLY

        // Fetch domain events starting from the calculated sequence number (offset)
        val events = domainEventStreamProvider
                .getDomainEventStream(query.entityId, startIndex.toLong()) ?: emptyList()

        // If the result is empty, return an empty result but still indicate position
        if (events.isEmpty()) {
            return DomainEventsResult(
                    entityId = query.entityId,
                    entityType = "",
                    domainEvents = emptyList(),
                    page = query.page,
                    pageSize = query.pageSize,
                    totalCount = startIndex.toLong() // Total so far is up to this offset
            )
        }

        // Map the first 'pageSize' events into the response model
        val pagedEvents = events
                .asSequence()
                .take(query.pageSize)
                .map { event ->
                    DomainEvent(
                            sequenceNumber = event.sequenceNumber,
                            timestamp = event.timestamp,
                            payloadType = event.payloadType.toString(),
                            payload = if (includePayload)
                                serializer.serialize(event.payload, String::class.java).data
                            else ""
                    )
                }
                .toCollection(ArrayList())

        // Total count is approximated as startIndex + number of events returned
        val totalEvents = startIndex + events.size

        return DomainEventsResult(
                entityId = events.first().aggregateIdentifier,
                entityType = events.first().type,
                domainEvents = pagedEvents,
                page = query.page,
                pageSize = query.pageSize,
                totalCount = totalEvents.toLong()
        )
    }


    private fun handleEntityStateAtSequenceQuery(query: EntityStateAtSequenceQuery): EntityStateResult {
        logger.debug("Handling AxonIQ Console ENTITY_STATE_AT_SEQUENCE query for request [{}]", query)
        takeIf {
            logger.debug("Access mode is set on: {}.", domainEventAccessMode)
            domainEventAccessMode == DomainEventAccessMode.FULL ||
                    domainEventAccessMode == DomainEventAccessMode.LOAD_DOMAIN_STATE_ONLY
        }?.let {
            domainEventStreamProvider.loadDomainStateAtSequence<String>(
                    type = query.type,
                    entityIdentifier = query.entityId,
                    maxSequenceNumber = query.maxSequenceNumber)?.let {
                return EntityStateResult(
                        type = query.type,
                        entityId = query.entityId,
                        maxSequenceNumber = query.maxSequenceNumber,
                        state = it
                )
            } ?: throw IllegalArgumentException("Could not load domain state for entityId: ${query.entityId}")
        } ?: throw IllegalArgumentException("Access mode is set on: $domainEventAccessMode. Cannot load the domain.")
    }
}