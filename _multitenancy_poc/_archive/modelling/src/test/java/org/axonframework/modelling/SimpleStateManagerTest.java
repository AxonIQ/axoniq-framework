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

package org.axonframework.modelling;

import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.modelling.repository.Repository;
import org.axonframework.modelling.repository.SimpleRepository;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

class SimpleStateManagerTest {

    private final Repository<String, Integer> repository = new SimpleRepository<>(
            String.class,
            Integer.class,
            (id, context) -> CompletableFuture.completedFuture(Integer.parseInt(id)),
            (id, entity, context) -> CompletableFuture.completedFuture(null)
    );

    @Test
    void registerAllowsEntitiesToBeLoadedFromTheRepository() {
        // given
        StateManager stateManager = SimpleStateManager.named("test")
                                                      .register(repository);

        // when
        var state = stateManager.loadEntity(Integer.class, "42", new StubProcessingContext()).join();

        // then
        assertEquals(42, state);
    }

    @Test
    void throwsExceptionOnMissingModelDefinition() {
        // given
        StateManager testSubject = SimpleStateManager.named("test");

        // when & then
        assertThrows(MissingRepositoryException.class,
                     () -> testSubject.loadEntity(Integer.class, "42", new StubProcessingContext()).join());
    }

    @Test
    void canRegisterEachModelClassOnlyOnce() {
        // given
        StateManager builder = SimpleStateManager.named("test")
                                                 .register(repository);

        // when & then
        assertThrows(RepositoryAlreadyRegisteredException.class, () -> builder.register(repository));
    }

    @Test
    void canRetrieveRegisteredEntities() {
        // given
        StateManager stateManager = SimpleStateManager.named("test")
                                                      .register(repository);

        // when
        var registeredTypes = stateManager.registeredEntities();

        // then
        assertEquals(1, registeredTypes.size());
        assertTrue(registeredTypes.contains(Integer.class));
    }

    @Test
    void canGetRepositoryForRegisteredType() {
        // given
        StateManager stateManager = SimpleStateManager.named("test")
                                                      .register(repository);

        // when
        var result = stateManager.repository(Integer.class, String.class);

        // then
        assertEquals(repository, result);
    }

    @Test
    void returnsNullIfTryingToGetRepositoryForUnregisteredType() {
        // given
        StateManager stateManager = SimpleStateManager.named("test")
                                                      .register(repository);

        // when & then
        assertNull(stateManager.repository(String.class, Integer.class));
    }


    @Test
    void returnsAllRegisteredIdentifiersForAnEntity() {
        // given
        SimpleRepository<String, MyFirstImplementingEntity> repository = new SimpleRepository<>(
                String.class,
                MyFirstImplementingEntity.class,
                (id, context) -> CompletableFuture.completedFuture(new MyFirstImplementingEntity()),
                (id, entity, context) -> CompletableFuture.completedFuture(null)
        );

        SimpleRepository<Integer, MyFirstImplementingEntity> repository2 = new SimpleRepository<>(
                Integer.class,
                MyFirstImplementingEntity.class,
                (id, context) -> CompletableFuture.completedFuture(new MyFirstImplementingEntity()),
                (id, entity, context) -> CompletableFuture.completedFuture(null)
        );

        StateManager stateManager = SimpleStateManager.named("test")
                                                      .register(repository)
                                                      .register(repository2);

        // when
        var result = stateManager.registeredIdsFor(MyFirstImplementingEntity.class);

        // then
        assertEquals(2, result.size());
        assertTrue(result.contains(String.class));
        assertTrue(result.contains(Integer.class));
    }

    @Nested
    class PolymorphicRepositories {

        @Test
        void canRequestMoreSpecificEntityThanDefinedInRepositoryIfTypeMatches() {
            // given
            SimpleRepository<String, MySuperEntity> repository = new SimpleRepository<>(
                    String.class,
                    MySuperEntity.class,
                    (id, context) -> CompletableFuture.completedFuture(new MyFirstImplementingEntity()),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );
            StateManager stateManager = SimpleStateManager.named("test")
                                                          .register(repository);

            // when
            var result = stateManager.loadManagedEntity(MyFirstImplementingEntity.class,
                                                        "42",
                                                        new StubProcessingContext()).join();

            // then
            assertNotNull(result.entity());
        }

        @Test
        void canNotRequestLessSpecificEntityThanDefinedInRepository() {
            // given
            SimpleRepository<String, MyFirstImplementingEntity> repository = new SimpleRepository<>(
                    String.class,
                    MyFirstImplementingEntity.class,
                    (id, context) -> CompletableFuture.completedFuture(new MyFirstImplementingEntity()),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );
            StateManager stateManager = SimpleStateManager.named("test")
                                                          .register(repository);

            // when & then
            assertThrows(MissingRepositoryException.class,
                         () -> stateManager.loadManagedEntity(MySuperEntity.class, "42", new StubProcessingContext())
                                           .join());
        }

        @Test
        void canRequestAnEntityWithAMoreSpecificIdentifierThanDefinedByRepository() {
            // given
            SimpleRepository<MySuperId, MySuperEntity> repository = new SimpleRepository<>(
                    MySuperId.class,
                    MySuperEntity.class,
                    (id, context) -> CompletableFuture.completedFuture(new MySuperEntity()),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );
            StateManager stateManager = SimpleStateManager.named("test")
                                                          .register(repository);

            // when
            var result = stateManager.loadManagedEntity(MySuperEntity.class,
                                                        new MySuperId(),
                                                        new StubProcessingContext()).join();

            // then
            assertNotNull(result.entity());
        }

        @Test
        void canNotRequestAnEntityWithALessSpecificIdentifierThanDefinedByRepository() {
            // given
            SimpleRepository<MyFirstImplementingId, MySuperEntity> repository = new SimpleRepository<>(
                    MyFirstImplementingId.class,
                    MySuperEntity.class,
                    (id, context) -> CompletableFuture.completedFuture(new MySuperEntity()),
                    (id, entity, context) -> CompletableFuture.completedFuture(null));
            StateManager stateManager = SimpleStateManager.named("test")
                                                          .register(repository);

            // when & then
            assertThrows(MissingRepositoryException.class,
                         () -> stateManager.loadManagedEntity(MySuperEntity.class,
                                                              new MySuperId(),
                                                              new StubProcessingContext()).join());
        }

        @Test
        void canRegisterTwoRepositoriesWithSameSuperClass() {
            // given
            SimpleRepository<String, MyFirstImplementingEntity> repository = new SimpleRepository<>(
                    String.class,
                    MyFirstImplementingEntity.class,
                    (id, context) -> CompletableFuture.completedFuture(new MyFirstImplementingEntity()),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );

            SimpleRepository<String, MySecondImplementingEntity> repository2 = new SimpleRepository<>(
                    String.class,
                    MySecondImplementingEntity.class,
                    (id, context) -> CompletableFuture.completedFuture(new MySecondImplementingEntity()),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );

            StateManager stateManager = SimpleStateManager.named("test")
                                                          .register(repository)
                                                          .register(repository2);

            // when
            var result = stateManager.loadManagedEntity(MyFirstImplementingEntity.class,
                                                        "42",
                                                        new StubProcessingContext()).join();
            var result2 = stateManager.loadManagedEntity(MySecondImplementingEntity.class,
                                                         "42",
                                                         new StubProcessingContext()).join();

            // then
            assertNotNull(result.entity());
            assertNotNull(result2.entity());
        }

        @Test
        void requestingASpecificClassWhileLoaderReturnsSuperclassShouldFail() {
            // given
            SimpleRepository<String, MySuperEntity> repository = new SimpleRepository<>(
                    String.class,
                    MySuperEntity.class,
                    (id, context) -> CompletableFuture.completedFuture(new MySuperEntity()),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );
            StateManager stateManager = SimpleStateManager.named("test")
                                                          .register(repository);

            // when & then
            var exception = assertThrows(CompletionException.class, () -> stateManager.loadManagedEntity(
                    MyFirstImplementingEntity.class,
                    "42",
                    new StubProcessingContext()).join());
            assertInstanceOf(LoadedEntityNotOfExpectedTypeException.class, exception.getCause());
        }

        @Test
        void canNotRegisterSubclassOfAlreadyRegisteredEntity() {
            // given
            SimpleRepository<String, MySuperEntity> repository = new SimpleRepository<>(
                    String.class,
                    MySuperEntity.class,
                    (id, context) -> CompletableFuture.completedFuture(new MySuperEntity()),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );

            SimpleRepository<String, MyFirstImplementingEntity> repository2 = new SimpleRepository<>(
                    String.class,
                    MyFirstImplementingEntity.class,
                    (id, context) -> CompletableFuture.completedFuture(new MyFirstImplementingEntity()),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );

            StateManager builder = SimpleStateManager.named("test").register(repository);

            // when & then
            assertThrows(RepositoryAlreadyRegisteredException.class, () -> builder.register(repository2));
        }


        @Test
        void canNotRegisterSuperclassOfAlreadyRegisteredEntity() {
            // given
            SimpleRepository<String, MyFirstImplementingEntity> repository = new SimpleRepository<>(
                    String.class,
                    MyFirstImplementingEntity.class,
                    (id, context) -> CompletableFuture.completedFuture(new MyFirstImplementingEntity()),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );
            SimpleRepository<String, MySuperEntity> repository2 = new SimpleRepository<>(
                    String.class,
                    MySuperEntity.class,
                    (id, context) -> CompletableFuture.completedFuture(new MySuperEntity()),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );


            StateManager builder = SimpleStateManager.named("test")
                                                     .register(repository);

            // when & then
            assertThrows(RepositoryAlreadyRegisteredException.class, () -> builder.register(repository2));
        }

        @Test
        void canRegisterAndLoadSameEntitywithDifferentIdentifierClasses() {
            // given
            SimpleRepository<String, MyFirstImplementingEntity> repository = new SimpleRepository<>(
                    String.class,
                    MyFirstImplementingEntity.class,
                    (id, context) -> CompletableFuture.completedFuture(new MyFirstImplementingEntity()),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );

            SimpleRepository<Integer, MyFirstImplementingEntity> repository2 = new SimpleRepository<>(
                    Integer.class,
                    MyFirstImplementingEntity.class,
                    (id, context) -> CompletableFuture.completedFuture(new MyFirstImplementingEntity()),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );

            StateManager stateManager = SimpleStateManager.named("test")
                                                          .register(repository)
                                                          .register(repository2);

            // when
            var result = stateManager.loadManagedEntity(MyFirstImplementingEntity.class,
                                                        "42",
                                                        new StubProcessingContext()).join();
            var result2 = stateManager.loadManagedEntity(MyFirstImplementingEntity.class,
                                                         42,
                                                         new StubProcessingContext()).join();

            // then
            assertNotNull(result.entity());
            assertNotNull(result2.entity());
        }

        @Test
        void canNotRegisterSecondIdClassForSameEntityTypeWhichIsASuperClassOfFirstIdClass() {
            // given
            SimpleRepository<MyFirstImplementingEntity, String> repository = new SimpleRepository<>(
                    MyFirstImplementingEntity.class,
                    String.class,
                    (id, context) -> CompletableFuture.completedFuture("42"),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );

            SimpleRepository<MySuperEntity, String> repository2 = new SimpleRepository<>(
                    MySuperEntity.class,
                    String.class,
                    (id, context) -> CompletableFuture.completedFuture("42"),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );

            StateManager builder = SimpleStateManager.named("test")
                                                     .register(repository);

            // when & then
            assertThrows(RepositoryAlreadyRegisteredException.class, () -> builder.register(repository2));
        }


        @Test
        void canNotRegisterSecondIdClassForSameEntityTypeWhichIsASubclassOfFirstIdClass() {
            // given
            SimpleRepository<MySuperEntity, String> repository = new SimpleRepository<>(
                    MySuperEntity.class,
                    String.class,
                    (id, context) -> CompletableFuture.completedFuture("42"),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );

            SimpleRepository<MyFirstImplementingEntity, String> repository2 = new SimpleRepository<>(
                    MyFirstImplementingEntity.class,
                    String.class,
                    (id, context) -> CompletableFuture.completedFuture("42"),
                    (id, entity, context) -> CompletableFuture.completedFuture(null)
            );

            StateManager builder = SimpleStateManager.named("test")
                                                     .register(repository);

            // when & then
            assertThrows(RepositoryAlreadyRegisteredException.class, () -> builder.register(repository2));
        }
    }


    class MySuperEntity {

    }

    class MyFirstImplementingEntity extends MySuperEntity {

    }

    class MySecondImplementingEntity extends MySuperEntity {

    }

    class MySuperId {

    }

    class MyFirstImplementingId extends MySuperId {

    }
}