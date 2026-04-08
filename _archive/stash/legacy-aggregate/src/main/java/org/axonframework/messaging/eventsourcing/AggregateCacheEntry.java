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

import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.modelling.command.RepositoryProvider;
import org.axonframework.modelling.command.inspection.AggregateModel;

public class AggregateCacheEntry<T> {

    private final T aggregateRoot;
    private final Long version;
    private final boolean deleted;
//    private final SnapshotTrigger snapshotTrigger;

    private final transient EventSourcedAggregate<T> aggregate;

    public AggregateCacheEntry(EventSourcedAggregate<T> aggregate) {
        this.aggregate = aggregate;
        this.aggregateRoot = aggregate.getAggregateRoot();
        this.version = aggregate.version();
        this.deleted = aggregate.isDeleted();
//        this.snapshotTrigger = NoSnapshotTriggerDefinition.TRIGGER;
    }

//    public EventSourcedAggregate<T> recreateAggregate(AggregateModel<T> model,
//                                                      LegacyEventStore eventStore,
//                                                      SnapshotTriggerDefinition snapshotTriggerDefinition) {
//        return recreateAggregate(model, eventStore, null, snapshotTriggerDefinition);
//    }

    public EventSourcedAggregate<T> recreateAggregate(AggregateModel<T> model,
                                                      EventStore eventStore,
                                                      RepositoryProvider repositoryProvider/*,
                                                      SnapshotTriggerDefinition snapshotTriggerDefinition*/) {
        if (aggregate != null) {
            return aggregate;
        }
//        return EventSourcedAggregate.reconstruct(aggregateRoot, model, version, deleted, eventStore, repositoryProvider,
//                                                 snapshotTriggerDefinition
//                                                         .reconfigure(aggregateRoot.getClass(), this.snapshotTrigger)
//        );
        return null;
    }
}
