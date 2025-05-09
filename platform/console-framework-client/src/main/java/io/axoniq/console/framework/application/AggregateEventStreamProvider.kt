package io.axoniq.console.framework.application

import org.axonframework.common.ReflectionUtils
import org.axonframework.config.AggregateConfiguration
import org.axonframework.config.Configuration
import org.axonframework.eventhandling.DomainEventMessage
import org.axonframework.eventhandling.EventMessage
import org.axonframework.eventsourcing.AggregateFactory
import org.axonframework.eventsourcing.EventSourcedAggregate
import org.axonframework.eventsourcing.SnapshotTrigger
import org.axonframework.eventsourcing.eventstore.AbstractEventStorageEngine
import org.axonframework.eventsourcing.eventstore.DomainEventStream
import org.axonframework.modelling.command.Repository
import org.axonframework.modelling.command.RepositoryProvider
import org.axonframework.modelling.command.inspection.AggregateModel

class AggregateEventStreamProvider(
        private val configuration: Configuration
) {

    fun getDomainEventStream(aggregateIdentifier: String): List<DomainEventMessage<*>>? =
            configuration.eventStore().readEvents(aggregateIdentifier, 0).asStream().toList()


    fun <T> loadForAggregate(type: String, identifier: String, maxSequenceNumber: Long): String? {
        val aggregateConfiguration = configuration.modules
                .filterIsInstance<AggregateConfiguration<*>>()
                .firstOrNull { it.aggregateType().simpleName == type }
                ?: throw IllegalArgumentException("No aggregate found for type $type")

        val eventStore = configuration.eventStore()

        val factory: AggregateFactory<T> = aggregateConfiguration.aggregateFactory() as AggregateFactory<T>
        val model: AggregateModel<T> = aggregateConfiguration.repository().getPropertyValue<AggregateModel<T>>("aggregateModel")
                ?: throw IllegalArgumentException("No aggregate model found for type $type")

        val stream = readEvents(identifier)
        val loadingAggregate: EventSourcedAggregate<T> = EventSourcedAggregate
                .initialize(factory.createAggregateRoot(identifier, stream.peek()),
                        model,
                        eventStore,
                        object : RepositoryProvider {
                            override fun <T> repositoryFor(aggregateType: Class<T>): Repository<T> {
                                return aggregateConfiguration.repository() as Repository<T>
                            }

                        },
                        object : SnapshotTrigger {
                            override fun eventHandled(p0: EventMessage<*>) {
                                // Do nothing
                            }

                            override fun initializationFinished() {
                                // Do nothing
                            }
                        })

        loadingAggregate.initializeState(stream.filter { it.sequenceNumber <= maxSequenceNumber })

        return configuration.serializer().serialize(loadingAggregate.aggregateRoot, String::class.java).data
    }

    private fun readEvents(identifier: String): DomainEventStream =
            configuration.eventStore()
                    .getPropertyValue<AbstractEventStorageEngine>("storageEngine")
                    ?.readEvents(identifier, 0)
                    ?: throw IllegalStateException("Unable to find AbstractEventStorageEngine in event store")

    @Suppress("UNCHECKED_CAST")
    private inline fun <reified T> Any.getPropertyValue(fieldName: String): T? =
            ReflectionUtils.fieldsOf(this::class.java)
                    .firstOrNull { it.name == fieldName }
                    ?.let { ReflectionUtils.getMemberValue(it, this) as? T }
}
