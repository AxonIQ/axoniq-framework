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

package org.axonframework.integrationtests.polymorphic;

import org.jspecify.annotations.NonNull;
import jakarta.persistence.EntityManager;
import org.axonframework.messaging.eventsourcing.LegacyEventSourcingRepository;
import org.axonframework.modelling.command.Repository;
import org.axonframework.modelling.command.RepositoryProvider;
import org.junit.jupiter.api.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Tests for ES aggregate polymorphism.
 *
 * @author Milan Savic
 */
@Disabled("TODO #3061 - Revisit Aggregate Polymorphism")
public class PolymorphicESAggregateAnnotationCommandHandlerTest
        extends AbstractPolymorphicAggregateAnnotationCommandHandlerTestSuite {

    private static final Map<Class<?>, Repository<?>> repositories = new HashMap<>();

    @Override
    public <T> Repository<T> repository(Class<T> aggregateType,
                                        Set<Class<? extends T>> subTypes,
                                        EntityManager entityManager) {
        LegacyEventSourcingRepository<T> repository = LegacyEventSourcingRepository
                .builder(aggregateType)
                .subtypes(subTypes)
//                .eventStore(LegacyEmbeddedEventStore.builder()
//                                                    .storageEngine(new LegacyInMemoryEventStorageEngine())
//                                                    .build())
                .repositoryProvider(new RepositoryProvider() {
                    @Override
                    public @NonNull <R> Repository<R> repositoryFor(@NonNull Class<R> aggregateType) {
                        //noinspection unchecked
                        return (Repository<R>) repositories.get(aggregateType);
                    }
                })
                .build();
        repositories.put(aggregateType, repository);
        return repository;
    }
}
