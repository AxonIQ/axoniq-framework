/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package org.axonframework.modelling.saga;

import org.axonframework.common.FutureUtils;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.MetadataValue;
import org.axonframework.messaging.core.interception.annotation.ExceptionHandler;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.replay.GenericResetContext;
import org.axonframework.messaging.eventhandling.replay.ResetNotSupportedException;
import org.axonframework.modelling.saga.repository.AnnotatedSagaRepository;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.modelling.saga.repository.inmemory.InMemorySagaStore;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.axonframework.messaging.eventhandling.EventTestUtils.asEventMessage;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link AnnotatedSagaManager}.
 *
 * @author Allard Buijze
 */
public class AnnotatedSagaManagerTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private AnnotatedSagaRepository<MyTestSaga> sagaRepository;
    private InMemorySagaStore sagaStore;
    private UnitOfWorkFactory unitOfWorkFactory;

    private AnnotatedSagaManager<MyTestSaga> testSubject;

    @BeforeEach
    void setUp() {
        sagaStore = new InMemorySagaStore();
        sagaRepository = spy(
                AnnotatedSagaRepository.<MyTestSaga>builder()
                                       .sagaType(MyTestSaga.class)
                                       .sagaStore(sagaStore)
                                       .build()
        );
        unitOfWorkFactory = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE);
        testSubject = AnnotatedSagaManager.<MyTestSaga>builder()
                                          .sagaRepository(sagaRepository)
                                          .sagaType(MyTestSaga.class)
                                          .sagaFactory(MyTestSaga::new)
                                          .build();
    }

    @Test
    void creationPolicy_NoneExists() throws Exception {
        handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("123")));
        assertThat(repositoryContents("123")).hasSize(1);
    }

    @Test
    void creationPolicy_OneAlreadyExists() throws Exception {
        handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("123")));
        handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("123")));
        assertThat(repositoryContents("123")).hasSize(1);
    }

    @Test
    void handleUnrelatedEvent() throws Exception {
        handle(new GenericEventMessage(new MessageType("event"), "Unrelated"));
        verify(sagaRepository, never()).find(isNull(), any());
    }

    @Test
    void creationPolicy_CreationForced() throws Exception {
        StartingEvent startingEvent = new StartingEvent("123");
        handle(new GenericEventMessage(new MessageType("event"), startingEvent));
        handle(new GenericEventMessage(new MessageType("event"), new ForcingStartEvent("123")));
        Collection<MyTestSaga> sagas = repositoryContents("123");
        assertThat(sagas).hasSize(2);
        for (MyTestSaga saga : sagas) {
            if (saga.getCapturedEvents().contains(startingEvent)) {
                assertThat(saga.getCapturedEvents()).hasSize(2);
            }
            assertThat(saga.getCapturedEvents().isEmpty()).isFalse();
        }
    }

    @Test
    void creationPolicy_SagaNotCreated() throws Exception {
        handle(new GenericEventMessage(new MessageType("event"), new MiddleEvent("123")));
        assertThat(repositoryContents("123")).hasSize(0);
    }

    @Test
    void mostSpecificHandlerEvaluatedFirst() throws Exception {
        handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("12")));
        handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("23")));
        assertThat(repositoryContents("12")).hasSize(1);
        assertThat(repositoryContents("23")).hasSize(1);

        handle(new GenericEventMessage(new MessageType("event"), new MiddleEvent("12")));
        handle(new GenericEventMessage(
                new MessageType("event"), new MiddleEvent("23"), singletonMap("catA", "value")
        ));
        assertThat(repositoryContents("12").iterator().next().getSpecificHandlerInvocations()).isEqualTo(0);
        assertThat(repositoryContents("23").iterator().next().getSpecificHandlerInvocations()).isEqualTo(1);
    }

    @Test
    void nullAssociationValueIsIgnored() throws Exception {
        handle(new GenericEventMessage(new MessageType("event"), new StartingEvent(null)));

        verify(sagaRepository, never()).find(isNull(), any());
    }

    @Test
    void lifecycle_DestroyedOnEnd() throws Exception {
        handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("12")));
        handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("23")));
        handle(new GenericEventMessage(new MessageType("event"), new MiddleEvent("12")));
        handle(new GenericEventMessage(
                new MessageType("event"), new MiddleEvent("23"), singletonMap("catA", "value")
        ));

        assertThat(repositoryContents("12")).hasSize(1);
        assertThat(repositoryContents("23")).hasSize(1);
        assertThat(repositoryContents("12").iterator().next().getSpecificHandlerInvocations()).isEqualTo(0);
        assertThat(repositoryContents("23").iterator().next().getSpecificHandlerInvocations()).isEqualTo(1);
        handle(new GenericEventMessage(new MessageType("event"), new EndingEvent("12")));
        assertThat(repositoryContents("23")).hasSize(1);
        assertThat(repositoryContents("12")).hasSize(0);
        handle(new GenericEventMessage(new MessageType("event"), new EndingEvent("23")));
        assertThat(repositoryContents("23")).hasSize(0);
        assertThat(repositoryContents("12")).hasSize(0);
    }

    @Test
    void nullAssociationValueDoesNotThrowNullPointer() throws Exception {
        handle(asEventMessage(new StartingEvent(null)));
    }

    @Test
    void lifeCycle_ExistingInstanceIgnoresEvent() throws Exception {
        handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("12")));
        handle(new GenericEventMessage(new MessageType("event"), new UnrelatedDomainEvent()));
        assertThat(repositoryContents("12")).hasSize(1);
        assertThat(repositoryContents("12").iterator().next().getCapturedEvents()).hasSize(1);
    }

    @Test
    void lifeCycle_IgnoredEventDoesNotCreateInstance() throws Exception {
        handle(new GenericEventMessage(new MessageType("event"), new UnrelatedDomainEvent()));
        assertThat(repositoryContents("12")).hasSize(0);
    }

    @Test
    void doesNotSupportReset() {
        assertThat(testSubject.supportsReset()).isFalse();
    }

    @Test
    void handlingAResetContextThrowsResetNotSupportedException() {
        var resetContext = new GenericResetContext(new MessageType(String.class), "reset-info");

        assertThatThrownBy(
                () -> testSubject.handle(resetContext, StubProcessingContext.forMessage(resetContext))
        ).isInstanceOf(ResetNotSupportedException.class);
    }

    /**
     * A {@link SagaLifecycle} parameter is how an Axon Framework 4 saga's calls to the static
     * {@code SagaLifecycle.associateWith(...)} and {@code SagaLifecycle.end()} are expressed in Axon Framework 5, which
     * has no thread local to read them from. It is therefore the migration path for every Axon Framework 4 saga that
     * manages its own associations, and a saga declaring one has to be exactly as visible to the manager as any other.
     * <p>
     * It is easy to break without noticing. The manager resolves handlers through the metamodel to extract the
     * {@link AssociationValue AssociationValues} it looks sagas up by and to read the
     * {@link SagaCreationPolicy}, and it does so before any saga has registered itself on the
     * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} - when starting one, before a saga exists
     * at all. A {@code ParameterResolver} that reports no match until the {@link SagaLifecycle#RESOURCE_KEY} resource
     * is present therefore removes the handler from the metamodel entirely, which leaves nothing to search on and a
     * {@link SagaCreationPolicy#NONE} policy. The saga is never started and never found, with nothing thrown and
     * nothing logged.
     * <p>
     * These tests exist because that is unobservable anywhere narrower: a resolver test can only exercise
     * {@code matches} directly, and every other saga fixture in this module declares no such parameter.
     */
    @Nested
    class SagaLifecycleInjection {

        private AnnotatedSagaManager<LifecycleInjectingTestSaga> lifecycleTestSubject;

        @BeforeEach
        void setUp() {
            lifecycleTestSubject =
                    AnnotatedSagaManager.<LifecycleInjectingTestSaga>builder()
                                        .sagaRepository(AnnotatedSagaRepository.<LifecycleInjectingTestSaga>builder()
                                                                              .sagaType(LifecycleInjectingTestSaga.class)
                                                                              .sagaStore(sagaStore)
                                                                              .build())
                                        .sagaType(LifecycleInjectingTestSaga.class)
                                        .sagaFactory(LifecycleInjectingTestSaga::new)
                                        .build();
        }

        @Test
        void aSagaWhoseStartingHandlerDeclaresASagaLifecycleIsStarted() {
            handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("123")));

            assertThat(sagaStore.findSagas(LifecycleInjectingTestSaga.class,
                                           new AssociationValue("myIdentifier", "123"))).hasSize(1);
        }

        @Test
        void theAssociationTheHandlerAddedThroughTheLifecycleIsStored() {
            handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("123")));

            assertThat(sagaStore.findSagas(LifecycleInjectingTestSaga.class,
                                           new AssociationValue("secondaryIdentifier", "secondary-123"))).hasSize(1);
        }

        @Test
        void theSagaIsFoundAgainByTheAssociationItAddedItself() {
            handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("123")));

            // a second starting event for the same identifier must not create a second saga, which it only avoids if
            // the manager could see the handler and find the existing saga through it
            handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("123")));

            assertThat(sagaStore.findSagas(LifecycleInjectingTestSaga.class,
                                           new AssociationValue("myIdentifier", "123"))).hasSize(1);
        }

        private void handle(EventMessage event) {
            UnitOfWork unitOfWork = unitOfWorkFactory.create();
            unitOfWork.runOnInvocation(context -> lifecycleTestSubject.handle(event, context));
            FutureUtils.joinAndUnwrap(unitOfWork.execute(), TIMEOUT);
        }
    }

    /**
     * A saga that suppresses its own handler failure through an {@link ExceptionHandler} reaches the outcome the Axon
     * Framework 4 {@code ListenerInvocationErrorHandler} default produced: the unit of work commits, the saga is
     * stored, and the saga counts as having taken the event so {@link SagaCreationPolicy#IF_NONE_FOUND} does not start
     * a second one.
     */
    @Nested
    class SuppressedHandlerFailures {

        private AnnotatedSagaManager<SuppressingTestSaga> suppressingTestSubject;

        @BeforeEach
        void setUp() {
            suppressingTestSubject =
                    AnnotatedSagaManager.<SuppressingTestSaga>builder()
                                        .sagaRepository(AnnotatedSagaRepository.<SuppressingTestSaga>builder()
                                                                               .sagaType(SuppressingTestSaga.class)
                                                                               .sagaStore(sagaStore)
                                                                               .build())
                                        .sagaType(SuppressingTestSaga.class)
                                        .sagaFactory(SuppressingTestSaga::new)
                                        .build();
        }

        @Test
        void aSuppressedFailureStillStoresTheSaga() {
            handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("123")));

            Collection<SuppressingTestSaga> sagas = suppressingRepositoryContents();
            assertThat(sagas).hasSize(1);
            SuppressingTestSaga saga = sagas.iterator().next();
            assertThat(saga.getHandlerInvocations()).isEqualTo(1);
            assertThat(saga.getSuppressedFailures()).isEqualTo(1);
        }

        @Test
        void aSuppressedFailureStillCountsAsInvoked() {
            handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("123")));
            handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("123")));

            Collection<SuppressingTestSaga> sagas = suppressingRepositoryContents();
            assertThat(sagas).hasSize(1);
            assertThat(sagas.iterator().next().getHandlerInvocations()).isEqualTo(2);
        }

        @Test
        void aSuppressedFailureInAnEndSagaHandlerStillEndsAndDeletesTheSaga() {
            handle(new GenericEventMessage(new MessageType("event"), new StartingEvent("123")));

            handle(new GenericEventMessage(new MessageType("event"), new EndingEvent("123")));

            // the saga ends despite its @EndSaga handler failing, as in Axon Framework 4, where SagaLifecycle.end()
            // ran in a finally block; suppressing the failure lets the unit of work commit, which deletes the saga
            assertThat(suppressingRepositoryContents()).hasSize(0);
        }

        private void handle(EventMessage event) {
            UnitOfWork unitOfWork = unitOfWorkFactory.create();
            unitOfWork.runOnInvocation(context -> suppressingTestSubject.handle(event, context));
            FutureUtils.joinAndUnwrap(unitOfWork.execute(), TIMEOUT);
        }

        private Collection<SuppressingTestSaga> suppressingRepositoryContents() {
            return sagaStore.findSagas(SuppressingTestSaga.class, new AssociationValue("myIdentifier", "123"))
                            .stream()
                            .map(id -> sagaStore.loadSaga(SuppressingTestSaga.class, id))
                            .map(SagaStore.Entry::saga)
                            .collect(Collectors.toList());
        }
    }

    private void handle(EventMessage event) {
        UnitOfWork unitOfWork = unitOfWorkFactory.create();
        unitOfWork.runOnInvocation(context -> testSubject.handle(event, context));
        FutureUtils.joinAndUnwrap(unitOfWork.execute(), TIMEOUT);
    }

    private Collection<MyTestSaga> repositoryContents(String lookupValue) {
        return sagaStore.findSagas(MyTestSaga.class, new AssociationValue("myIdentifier", lookupValue))
                        .stream()
                        .map(id -> sagaStore.loadSaga(MyTestSaga.class, id))
                        .map(SagaStore.Entry::saga)
                        .collect(Collectors.toList());
    }

    @SuppressWarnings("unused")
    public static class MyTestSaga {

        private final List<Object> capturedEvents = new LinkedList<>();
        private int specificHandlerInvocations = 0;

        @CustomStartingSagaEventHandler
        public void handleSomeEvent(StartingEvent event) {
            capturedEvents.add(event);
        }

        @StartSaga
        @SagaEventHandler(associationProperty = "myIdentifier")
        public void handleSomeEvent(SlowStartingEvent event) throws InterruptedException {
            event.getStartCdl().countDown();
            capturedEvents.add(event);
            Thread.sleep(event.getDuration());
        }

        @StartSaga(forceNew = true)
        @SagaEventHandler(associationProperty = "myIdentifier")
        public void handleSomeEvent(ForcingStartEvent event) {
            capturedEvents.add(event);
        }

        @CustomEndingSagaEventHandler
        public void handleSomeEvent(EndingEvent event) {
            capturedEvents.add(event);
        }

        @SagaEventHandler(associationProperty = "myIdentifier")
        public void handleMiddleEvent(MiddleEvent event) {
            capturedEvents.add(event);
        }

        @SagaEventHandler(associationProperty = "myIdentifier")
        public void handleSpecificMiddleEvent(MiddleEvent event,
                                              @MetadataValue(value = "catA", required = true) String category) {
            // this handler is more specific, but requires metadata that not all events might have
            capturedEvents.add(event);
            specificHandlerInvocations++;
        }

        public List<Object> getCapturedEvents() {
            return capturedEvents;
        }

        public int getSpecificHandlerInvocations() {
            return specificHandlerInvocations;
        }
    }

    @SuppressWarnings("unused")
    public static abstract class MyIdentifierEvent {

        private final String myIdentifier;

        public MyIdentifierEvent(String myIdentifier) {
            this.myIdentifier = myIdentifier;
        }

        public String getMyIdentifier() {
            return myIdentifier;
        }
    }

    public static class StartingEvent extends MyIdentifierEvent {

        public StartingEvent(String myIdentifier) {
            super(myIdentifier);
        }
    }

    public static class SlowStartingEvent extends StartingEvent {

        private final CountDownLatch startCdl;
        private final long duration;

        protected SlowStartingEvent(String myIdentifier, CountDownLatch startCdl, long duration) {
            super(myIdentifier);
            this.startCdl = startCdl;
            this.duration = duration;
        }

        public long getDuration() {
            return duration;
        }

        public CountDownLatch getStartCdl() {
            return startCdl;
        }
    }

    public static class ForcingStartEvent extends MyIdentifierEvent {

        public ForcingStartEvent(String myIdentifier) {
            super(myIdentifier);
        }
    }

    public static class EndingEvent extends MyIdentifierEvent {

        public EndingEvent(String myIdentifier) {
            super(myIdentifier);
        }
    }

    public static class MiddleEvent extends MyIdentifierEvent {

        public MiddleEvent(String myIdentifier) {
            super(myIdentifier);
        }
    }

    public static class UnrelatedDomainEvent {

    }

    @SuppressWarnings("unused")
    public static class LifecycleInjectingTestSaga {

        @StartSaga
        @SagaEventHandler(associationProperty = "myIdentifier")
        public void handleStartingEvent(StartingEvent event, SagaLifecycle lifecycle) {
            lifecycle.associateWith("secondaryIdentifier", "secondary-" + event.getMyIdentifier());
        }
    }

    @SuppressWarnings("unused")
    public static class SuppressingTestSaga {

        private int handlerInvocations = 0;
        private int suppressedFailures = 0;

        @StartSaga
        @SagaEventHandler(associationProperty = "myIdentifier")
        public void handleStartingEvent(StartingEvent event) {
            handlerInvocations++;
            throw new IllegalStateException("saga handler failed");
        }

        @EndSaga
        @SagaEventHandler(associationProperty = "myIdentifier")
        public void handleEndingEvent(EndingEvent event) {
            handlerInvocations++;
            throw new IllegalStateException("saga handler failed");
        }

        @ExceptionHandler
        public void on(IllegalStateException failure) {
            suppressedFailures++;
        }

        public int getHandlerInvocations() {
            return handlerInvocations;
        }

        public int getSuppressedFailures() {
            return suppressedFailures;
        }
    }
}
