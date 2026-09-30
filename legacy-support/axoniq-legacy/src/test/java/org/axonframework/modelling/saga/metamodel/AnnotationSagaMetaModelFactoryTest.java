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

package org.axonframework.modelling.saga.metamodel;

import org.axonframework.common.AxonException;
import org.axonframework.common.FutureUtils;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.interception.annotation.MessageHandlerInterceptorMemberChain;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.interception.annotation.NoMoreInterceptors;
import org.axonframework.messaging.core.interception.annotation.ExceptionHandler;
import org.axonframework.modelling.saga.AssociationValue;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.axonframework.messaging.eventhandling.EventTestUtils.asEventMessage;

class AnnotationSagaMetaModelFactoryTest {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private AnnotationSagaMetaModelFactory testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new AnnotationSagaMetaModelFactory();
    }

    @Test
    void inspectSaga() {
        SagaModel<MySaga> sagaModel = testSubject.modelOf(MySaga.class);

        EventMessage event = asEventMessage(new MySagaStartEvent("value"));
        Optional<AssociationValue> actual = sagaModel.resolveAssociation(event, StubProcessingContext.forMessage(event));
        assertThat(actual.isPresent()).isTrue();
        assertThat(actual.get().getValue()).isEqualTo("value");
        assertThat(actual.get().getKey()).isEqualTo("property");
    }

    @Test
    void chainedInterceptorShouldDefaultToNoMoreInterceptors() {
        SagaModel<MySaga> sagaModel = testSubject.modelOf(MySaga.class);

        MySaga saga = new MySaga();
        EventMessage event = asEventMessage(new MySagaStartEvent("foo"));
        Optional<MessageHandlingMember<? super MySaga>> handler = sagaModel
                .findHandlerMethods(event, StubProcessingContext.forMessage(event)).stream().findFirst();
        assertThat(handler.isPresent()).isTrue();
        MessageHandlerInterceptorMemberChain<MySaga> interceptorChain = testSubject.chainedInterceptor(MySaga.class);
        assertThatThrownBy(() -> FutureUtils.joinAndUnwrap(
                interceptorChain.handle(event, StubProcessingContext.forMessage(event), saga, handler.get())
                                .first()
                                .asCompletableFuture()
        )).isInstanceOf(FooException.class);
    }

    @Test
    void exceptionShouldBeCaughtByExceptionHandler() throws Exception {
        SagaModel<MySagaWithErrorHandler> sagaModel = testSubject.modelOf(MySagaWithErrorHandler.class);

        MySagaWithErrorHandler saga = new MySagaWithErrorHandler();
        EventMessage event = asEventMessage(new MySagaStartEvent("foo"));
        Optional<MessageHandlingMember<? super MySagaWithErrorHandler>> handler = sagaModel
                .findHandlerMethods(event, StubProcessingContext.forMessage(event)).stream().findFirst();
        assertThat(handler.isPresent()).isTrue();
        MessageHandlerInterceptorMemberChain<MySagaWithErrorHandler> interceptorChain = testSubject.chainedInterceptor(
                MySagaWithErrorHandler.class);
        var entry = FutureUtils.joinAndUnwrap(
                interceptorChain.handle(event, StubProcessingContext.forMessage(event), saga, handler.get())
                                .first()
                                .asCompletableFuture()
        );
        assertThat(entry).isNull();
    }

    @Test
    void messageHandlerInterceptorShouldDefaultToNoMoreInterceptors() {
        assertThat(testSubject.chainedInterceptor(MySaga.class).getClass()).isEqualTo(NoMoreInterceptors.class);
    }

    @Nested
    class SupportedEvents {

        @Test
        void returnsAQualifiedNameForEverySagaEventHandlerAnnotatedMethod() {
            SagaModel<MySaga> sagaModel = testSubject.modelOf(MySaga.class);

            assertThat(sagaModel.supportedEvents()).containsExactlyInAnyOrder(
                    new QualifiedName(MySagaStartEvent.class),
                    new QualifiedName(MySagaUpdateEvent.class),
                    new QualifiedName(MySagaEndEvent.class)
            );
        }

        @Test
        void returnsAnEmptySetForASagaWithoutEventHandlers() {
            SagaModel<SagaWithoutEventHandlers> sagaModel = testSubject.modelOf(SagaWithoutEventHandlers.class);

            assertThat(sagaModel.supportedEvents()).isEmpty();
        }

        @Test
        void includesInheritedSagaEventHandlerAnnotatedMethods() {
            SagaModel<MySagaWithErrorHandler> sagaModel = testSubject.modelOf(MySagaWithErrorHandler.class);

            assertThat(sagaModel.supportedEvents()).containsExactlyInAnyOrder(
                    new QualifiedName(MySagaStartEvent.class),
                    new QualifiedName(MySagaUpdateEvent.class),
                    new QualifiedName(MySagaEndEvent.class)
            );
        }
    }

    @Nested
    class PayloadTypeFor {

        @Test
        void returnsTheHandlerPayloadTypeForADeclaredEventName() {
            SagaModel<MySaga> sagaModel = testSubject.modelOf(MySaga.class);

            assertThat(sagaModel.payloadTypeFor(new QualifiedName(MySagaStartEvent.class)))
                    .contains(MySagaStartEvent.class);
        }

        @Test
        void returnsEmptyForAnEventNameWithoutAHandler() {
            SagaModel<MySaga> sagaModel = testSubject.modelOf(MySaga.class);

            assertThat(sagaModel.payloadTypeFor(new QualifiedName(String.class))).isEmpty();
        }
    }

    public static class MySaga {

        @StartSaga
        @SagaEventHandler(associationProperty = "property")
        public void handle(MySagaStartEvent event) {
            if ("foo".equals(event.getProperty())) {
                throw new FooException("value was foo");
            }
        }

        @SagaEventHandler(associationProperty = "property")
        public void handle(MySagaUpdateEvent event) {

        }

        @SagaEventHandler(associationProperty = "property")
        public void handle(MySagaEndEvent event) {

        }
    }

    public static class MySagaWithErrorHandler extends MySaga {

        @ExceptionHandler
        public void on(Exception e) {
            logger.info("caught", e);
        }
    }

    public static class SagaWithoutEventHandlers {

    }

    public abstract static class MySagaEvent {

        private final String property;

        public MySagaEvent(String property) {
            this.property = property;
        }

        public String getProperty() {
            return property;
        }
    }

    private static class MySagaStartEvent extends MySagaEvent {

        public MySagaStartEvent(String property) {
            super(property);
        }
    }

    private static class MySagaUpdateEvent extends MySagaEvent {

        public MySagaUpdateEvent(String property) {
            super(property);
        }
    }

    private static class MySagaEndEvent extends MySagaEvent {

        public MySagaEndEvent(String property) {
            super(property);
        }
    }

    private static class FooException extends AxonException {

        public FooException(String message) {
            super(message);
        }
    }
}
