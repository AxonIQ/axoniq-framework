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
import org.axonframework.messaging.eventsourcing.AbstractAggregateFactory;
import org.axonframework.modelling.command.inspection.AggregateModel;
import org.axonframework.modelling.command.inspection.AnnotatedAggregateMetaModelFactory;
import org.junit.jupiter.api.*;
import org.mockito.internal.util.collections.*;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test class validating the {@link AbstractAggregateFactory}.
 *
 * @author Steven van Beelen
 */
class AbstractAggregateFactoryTest {

    @Test
    void polymorphicFactoryConstructorBuildsAnticipatedAggregateModel() {
        //noinspection unchecked
        Set<Class<? extends RootAggregate>> subTypes = Sets.newSet(LeafOneAggregate.class, LeafTwoAggregate.class);

        AggregateModel<RootAggregate> expectedAggregateModel =
                AnnotatedAggregateMetaModelFactory.inspectAggregate(RootAggregate.class, subTypes);

        AbstractAggregateFactory<RootAggregate> testSubject = new TestAggregateFactory<>(RootAggregate.class, subTypes);

        AggregateModel<RootAggregate> resultAggregateModel = testSubject.aggregateModel();
        List<Class<?>> resultTypes = resultAggregateModel.types().collect(Collectors.toList());

        expectedAggregateModel.types().map(resultTypes::contains).forEach(Assertions::assertTrue);
    }

    private static class TestAggregateFactory<A> extends AbstractAggregateFactory<A> {

        protected TestAggregateFactory(Class<A> aggregateBaseType, Set<Class<? extends A>> aggregateSubTypes) {
            super(aggregateBaseType, aggregateSubTypes);
        }

        @Override
        protected A doCreateAggregate(String aggregateIdentifier,
                                      @SuppressWarnings("rawtypes") DomainEventMessage firstEvent) {
            return null;
        }
    }

    private static class RootAggregate {

    }

    private static class LeafOneAggregate extends RootAggregate {

    }

    private static class LeafTwoAggregate extends RootAggregate {

    }
}