/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.eventsourcing;

import org.axonframework.messaging.eventhandling.DomainEventMessage;
import org.axonframework.modelling.command.inspection.AggregateModel;
import org.axonframework.modelling.command.inspection.AnnotatedAggregateMetaModelFactory;

import java.util.Optional;
import java.util.Set;

/**
 * Abstract AggregateFactory implementation that is aware of snapshot events. If an incoming event is not a snapshot
 * event, creation is delegated to the subclass.
 *
 * @param <T> The type of Aggregate created by this factory
 * @author Allard Buijze
 * @since 2.0
 */
public abstract class AbstractAggregateFactory<T> implements AggregateFactory<T> {

    private final Class<T> aggregateBaseType;
    private final AggregateModel<T> aggregateModel;

    /**
     * Initialize an {@link AggregateFactory} for the given {@code aggregateBaseType}.
     * <p>
     * If a first event is an instance of this {@code aggregateBaseType}, it is recognised as a snapshot event.
     * Otherwise, the subclass is asked to instantiate a new aggregate root instance based on the first event.
     *
     * @param aggregateBaseType the base type of the aggregate roots created by this instance
     */
    protected AbstractAggregateFactory(Class<T> aggregateBaseType) {
        this(AnnotatedAggregateMetaModelFactory.inspectAggregate(aggregateBaseType));
    }

    /**
     * Initialize an {@link AggregateFactory} for the given polymorphic {@code aggregateBaseType} and it's {@code
     * aggregateSubTypes}.
     * <p>
     * If a first event is an instance of this {@code aggregateBaseType}, it is recognised as a snapshot event.
     * Otherwise, the subclass is asked to instantiate a new aggregate root instance based on the first event.
     *
     * @param aggregateBaseType the base type of the aggregate roots created by this instance
     * @param aggregateSubTypes a {@link Set} of sub types of the given {@code aggregateBaseType}
     */
    protected AbstractAggregateFactory(Class<T> aggregateBaseType, Set<Class<? extends T>> aggregateSubTypes) {
        this(AnnotatedAggregateMetaModelFactory.inspectAggregate(aggregateBaseType, aggregateSubTypes));
    }

    /**
     * Initializes an {@link AggregateFactory} for the given {@code aggregateModel}.
     * <p>
     * If a first event is an instance of any aggregate root within this {@code aggregateModel}, it is recognised as a
     * snapshot event. Otherwise, the subclass is asked to instantiate a new aggregate root instance based on the first
     * event.
     *
     * @param aggregateModel the model of aggregate to be created by this factory
     */
    protected AbstractAggregateFactory(AggregateModel<T> aggregateModel) {
        //noinspection unchecked
        this.aggregateBaseType = (Class<T>) aggregateModel.entityClass();
        this.aggregateModel = aggregateModel;
    }

    /**
     * Gets the aggregate model.
     *
     * @return the aggregate model
     */
    protected AggregateModel<T> aggregateModel() {
        return aggregateModel;
    }

    @Override
    public final T createAggregateRoot(String aggregateIdentifier, DomainEventMessage firstEvent) {
        return postProcessInstance(fromSnapshot(firstEvent).orElseGet(() -> doCreateAggregate(aggregateIdentifier, firstEvent)));
    }

    @SuppressWarnings("unchecked")
    private Optional<T> fromSnapshot(DomainEventMessage firstEvent) {
        if (aggregateModel.types().anyMatch(firstEvent.payloadType()::equals)) {
            return (Optional<T>) Optional.of(firstEvent.payload());
        }
        return Optional.empty();
    }

    /**
     * Perform any processing that must be done on an aggregate instance that was reconstructed from a Snapshot Event.
     * Implementations may choose to modify the existing instance, or return a new instance.
     * <p/>
     * This method can be safely overridden. This implementation does nothing.
     *
     * @param aggregate The aggregate to post-process.
     * @return The aggregate to initialize with the Event Stream
     */
    protected T postProcessInstance(T aggregate) {
        return aggregate;
    }

    /**
     * Create an uninitialized Aggregate instance with the given {@code aggregateIdentifier}. The given {@code
     * firstEvent} can be used to define the requirements of the aggregate to create.
     * <p/>
     * The given {@code firstEvent} is never a snapshot event.
     *
     * @param aggregateIdentifier The identifier of the aggregate to create
     * @param firstEvent          The first event in the Event Stream of the Aggregate
     * @return The aggregate instance to initialize with the Event Stream
     */
    protected abstract T doCreateAggregate(String aggregateIdentifier, DomainEventMessage firstEvent);

    @Override
    public Class<T> getAggregateType() {
        return aggregateBaseType;
    }
}
