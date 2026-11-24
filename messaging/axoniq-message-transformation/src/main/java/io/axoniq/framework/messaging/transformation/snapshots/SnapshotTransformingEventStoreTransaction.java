/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.snapshots;

import io.axoniq.framework.messaging.transformation.ChainConfigurationException;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.eventsourcing.eventstore.Position;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.api.SnapshotCandidate;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

/**
 * The {@link EventStoreTransaction} of a {@link SnapshotTransformingEventStore}, applying its chain to sourcing with
 * the {@link SourcingStrategy.Snapshot snapshot strategy}. Everything else is passed on to the delegate untouched.
 *
 * @author John Hendrikx
 * @since 5.5.0
 */
final class SnapshotTransformingEventStoreTransaction implements EventStoreTransaction {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotTransformingEventStoreTransaction.class);

    private final EventStoreTransaction delegate;
    private final SnapshotTransformerChain chain;
    private final Converter converter;
    private final ProcessingContext context;

    SnapshotTransformingEventStoreTransaction(EventStoreTransaction delegate,
                                              SnapshotTransformerChain chain,
                                              Converter converter,
                                              ProcessingContext context) {
        this.delegate = requireNonNull(delegate, "delegate may not be null");
        this.chain = requireNonNull(chain, "chain may not be null");
        this.converter = requireNonNull(converter, "converter may not be null");
        this.context = requireNonNull(context, "context may not be null");
    }

    @Override
    public MessageStream<? extends EventMessage> source(SourcingCondition condition,
                                                         @Nullable Consumer<Position> resumePositionCallback) {
        if (!(condition.strategy() instanceof SourcingStrategy.Snapshot requested)) {
            return delegate.source(condition, resumePositionCallback);
        }

        /*
         * The first candidate is the type the entity is currently declared with; the chain adds every identity it can
         * lift into that type, nearest first.
         */

        SnapshotCandidate current = requested.candidates().getFirst();
        List<VersionedType> targets = current.acceptableVersions().stream()
                                             .map(version -> VersionedType.of(current.qualifiedName(), version))
                                             .toList();
        Set<SnapshotCandidate> candidates = new LinkedHashSet<>(requested.candidates());
        SnapshotCandidate aggregateBasedCandidate = requested.aggregateBasedCandidate();

        for (VersionedType target : targets) {
            SnapshotTransformerChain.Lookup lookup = chain.lookupFor(target);

            candidates.addAll(lookup.candidates());

            if (aggregateBasedCandidate == null) {
                aggregateBasedCandidate = lookup.aggregateBasedCandidate();
            }
        }

        if (candidates.size() == requested.candidates().size()
                && Objects.equals(aggregateBasedCandidate, requested.aggregateBasedCandidate())) {
            return delegate.source(condition, resumePositionCallback);
        }

        Lifter lifter = new Lifter(current, targets, identitiesOf(candidates), aggregateBasedCandidate);
        SourcingStrategy.Snapshot widened = new SourcingStrategy.Snapshot(
                List.copyOf(candidates), aggregateBasedCandidate, requested.identifier(), requested.maximumPosition()
        );

        return delegate.source(SourcingCondition.conditionFor(widened, condition.criteria()), resumePositionCallback)
                       .mapMessage(lifter::liftIfSnapshot);
    }

    private static Set<VersionedType> identitiesOf(Set<SnapshotCandidate> candidates) {
        Set<VersionedType> identities = new LinkedHashSet<>();

        for (SnapshotCandidate candidate : candidates) {
            for (String version : candidate.acceptableVersions()) {
                identities.add(VersionedType.of(candidate.qualifiedName(), version));
            }
        }

        return identities;
    }

    @Override
    public void appendEvent(EventMessage eventMessage) {
        delegate.appendEvent(eventMessage);
    }

    @Override
    public void onAppend(Consumer<EventMessage> callback) {
        delegate.onAppend(callback);
    }

    @Override
    public void overrideAppendCondition(UnaryOperator<AppendCondition> conditionOverride) {
        delegate.overrideAppendCondition(conditionOverride);
    }

    @Override
    public ConsistencyMarker appendPosition() {
        return delegate.appendPosition();
    }

    /*
     * Lifts a snapshot found for one sourcing request into the entity's current type. The snapshot is passed on as
     * found when it already is of the current type, or when it cannot be lifted; the entity rejects the latter.
     */
    private final class Lifter {

        private final SnapshotCandidate current;
        private final List<VersionedType> targets;
        private final Set<VersionedType> identities;
        private final @Nullable SnapshotCandidate aggregateBasedCandidate;

        private Lifter(SnapshotCandidate current,
                       List<VersionedType> targets,
                       Set<VersionedType> identities,
                       @Nullable SnapshotCandidate aggregateBasedCandidate) {
            this.current = current;
            this.targets = targets;
            this.identities = identities;
            this.aggregateBasedCandidate = aggregateBasedCandidate;

            /*
             * A store reports an aggregate-based snapshot under its payload type and revision. When those coincide
             * with a candidate, a found snapshot cannot be told apart from one stored under that candidate.
             */

            if (aggregateBasedCandidate != null) {
                for (String version : aggregateBasedCandidate.acceptableVersions()) {
                    VersionedType aggregateBasedIdentity =
                            VersionedType.of(aggregateBasedCandidate.qualifiedName(), version);

                    if (identities.contains(aggregateBasedIdentity)) {
                        throw new ChainConfigurationException(
                                "The aggregate-based snapshot identity " + aggregateBasedIdentity + " coincides with a "
                                        + "snapshot identity of " + current.qualifiedName() + ", so a snapshot found "
                                        + "under it is ambiguous"
                        );
                    }
                }
            }
        }

        private EventMessage liftIfSnapshot(EventMessage message) {
            return message instanceof SnapshotEventMessage snapshotMessage ? lift(snapshotMessage) : message;
        }

        private EventMessage lift(SnapshotEventMessage message) {
            VersionedType found = message.snapshotType();

            if (found == null || isCurrent(found)) {
                return message;
            }

            boolean aggregateBased = !identities.contains(VersionedType.of(found.qualifiedName(), found.version()))
                    && aggregateBasedCandidate != null
                    && aggregateBasedCandidate.qualifiedName().equals(found.qualifiedName())
                    && aggregateBasedCandidate.accepts(found.version());
            Snapshot snapshot = message.payload();

            try {
                for (VersionedType target : targets) {
                    Object state = chain.lift(found, aggregateBased, snapshot.payload(), target, converter, context);

                    if (state != null) {
                        return new SnapshotEventMessage(
                                target.qualifiedName(),
                                new Snapshot(snapshot.position(), target.version(), state, snapshot.timestamp(),
                                             snapshot.metadata())
                        );
                    }
                }
            }
            catch (ConversionException e) {
                logger.warn("Snapshot of {} could not be read for lifting into {}, sourcing in full instead",
                            found, current.qualifiedName(), e);
            }

            return message;
        }

        private boolean isCurrent(VersionedType found) {
            return found.qualifiedName().equals(current.qualifiedName()) && current.accepts(found.version());
        }
    }
}
