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

package org.axonframework.config;

import org.junit.jupiter.api.extension.*;
import org.mockito.junit.jupiter.*;

// TODO #3097 - Fix as part of revamp of SagaConfigurer into workable ConfigurationEnhancer / Module
@ExtendWith(MockitoExtension.class)
class SagaConfigurerTest {

//    @AfterEach
//    void cleanup() {
//        TestSaga.counter.set(0);
//    }
//
//    @Test
//    void nullChecksOnSagaConfigurer() {
//        SagaConfigurer<Object> configurer = SagaConfigurer.forType(Object.class);
//        assertConfigurerNullCheck(() -> SagaConfigurer.forType(null), "Saga type should be checked for null");
//        assertConfigurerNullCheck(() -> configurer.configureSagaStore(null),
//                                  "Saga store builder should be checked for null");
//        assertConfigurerNullCheck(() -> configurer.configureSagaManager(null),
//                                  "Saga manager should be checked for null");
//        assertConfigurerNullCheck(() -> configurer.configureRepository(null),
//                                  "Saga repository should be checked for null");
//    }
//
//    @Test
//    void defaultConfiguration(
//            @Mock ListenerInvocationErrorHandler listenerInvocationErrorHandler,
//            @Mock SagaStore store
//    ) {
//        LegacyConfiguration configuration =
//                LegacyDefaultConfigurer.defaultConfiguration()
//                                       .eventProcessing(ep -> ep.registerSaga(Object.class))
//                                       .registerComponent(ListenerInvocationErrorHandler.class,
//                                                          c -> listenerInvocationErrorHandler
//                                       )
//                                       .registerComponent(SagaStore.class, c -> store)
//                                       .buildConfiguration();
//        SagaConfiguration<Object> sagaConfiguration = configuration.eventProcessingConfiguration().sagaConfiguration(
//                Object.class);
//
//        assertEquals("ObjectProcessor", sagaConfiguration.processingGroup());
//        assertEquals(Object.class, sagaConfiguration.type());
//        assertEquals(store, sagaConfiguration.store());
//        assertEquals(listenerInvocationErrorHandler, sagaConfiguration.listenerInvocationErrorHandler());
//    }
//
//    @Test
//    void customConfiguration(
//            @Mock SagaRepository<Object> repository,
//            @Mock AnnotatedSagaManager<Object> manager) {
//        SagaStore<Object> sagaStore = new InMemorySagaStore();
//        String processingGroup = "myProcessingGroup";
//
//        EventProcessingModule eventProcessingModule = new EventProcessingModule();
//        eventProcessingModule.registerSaga(Object.class, sc -> sc.configureSagaStore(c -> sagaStore)
//                                                                 .configureRepository(c -> repository)
//                                                                 .configureSagaManager(c -> manager));
//        eventProcessingModule.assignProcessingGroup("ObjectProcessor", processingGroup)
//                             .assignHandlerTypesMatching(processingGroup, clazz -> clazz.equals(Object.class));
//        LegacyConfiguration configuration = LegacyDefaultConfigurer.defaultConfiguration()
//                                                                   .registerModule(eventProcessingModule)
//                                                                   .buildConfiguration();
//
//        SagaConfiguration<Object> sagaConfiguration = configuration.eventProcessingConfiguration().sagaConfiguration(
//                Object.class);
//
//        assertEquals(Object.class, sagaConfiguration.type());
//        assertEquals(processingGroup, sagaConfiguration.processingGroup());
//        assertEquals(manager, sagaConfiguration.manager());
//        assertEquals(repository, sagaConfiguration.repository());
//        assertEquals(sagaStore, sagaConfiguration.store());
//    }
//
//
//    @Disabled("TODO #3443 - Adjust SagaRepository API to be async-native")
//    @Test
//    void deduplicateRegisterSaga() {
//        LegacyEmbeddedEventStore eventStore =
//                LegacyEmbeddedEventStore.builder()
//                                        .storageEngine(new LegacyInMemoryEventStorageEngine())
//                                        .build();
//        SagaStore<Object> sagaStore = new InMemorySagaStore();
//        EventProcessingModule eventProcessingModule = new EventProcessingModule();
//        eventProcessingModule
//                .registerSaga(TestSaga.class)
//                .registerSaga(TestSaga.class, sc -> sc.configureSagaStore(c -> sagaStore))
//                .registerSubscribingEventProcessor("testsaga", c -> eventStore);
//        LegacyConfiguration configuration = LegacyDefaultConfigurer.defaultConfiguration()
//                                                                   .configureEventStore(c -> eventStore)
//                                                                   .registerModule(eventProcessingModule)
//                                                                   .buildConfiguration();
//        configuration.start();
//        TestEvent testEvent = new TestEvent();
//        eventStore.publish(EventTestUtils.asEventMessage(testEvent));
//        Set<String> sagas = sagaStore.findSagas(TestSaga.class, new AssociationValue("id", testEvent.id.toString()));
//        assertEquals(1, sagas.size());
//        assertEquals(1, TestSaga.counter.get());
//    }
//
//    private void assertConfigurerNullCheck(Runnable r, String message) {
//        try {
//            r.run();
//            fail(message);
//        } catch (AxonConfigurationException ace) {
//            // we expect this exception
//        }
//    }
//
//    private static class TestEvent {
//
//        private final UUID id;
//
//        private TestEvent() {
//            id = UUID.randomUUID();
//        }
//
//        @SuppressWarnings("unused")
//        public UUID getId() {
//            return id;
//        }
//    }
//
//    @ProcessingGroup("testsaga")
//    public static class TestSaga {
//
//        static final AtomicInteger counter = new AtomicInteger();
//
//        @StartSaga
//        @SagaEventHandler(associationProperty = "id")
//        public void handleCreated(TestEvent event) {
//            counter.incrementAndGet();
//        }
//    }
}
