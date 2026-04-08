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

package org.axonframework.messaging.core.interception;

import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageDispatchInterceptorChain;
import org.axonframework.messaging.core.MessageHandlerInterceptorChain;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link BeanValidationInterceptor}.
 *
 * @author Allard Buijze
 */
class BeanValidationInterceptorTest {

    private BeanValidationInterceptor<Message> testSubject;

    private MessageHandlerInterceptorChain<Message> handlerInterceptorChain;
    private MessageDispatchInterceptorChain<Message> dispatchInterceptorChain;

    @BeforeEach
    void setUp() {
        testSubject = new BeanValidationInterceptor<>();
        handlerInterceptorChain = mock();
        dispatchInterceptorChain = mock();
    }

    @Test
    void validateSimpleObject() {
        Message message = new GenericMessage(new MessageType("message"), "Simple instance");
        ProcessingContext context = StubProcessingContext.forMessage(message);
        testSubject.interceptOnHandle(message, context, handlerInterceptorChain);
        verify(handlerInterceptorChain).proceed(message, context);
    }

    @Test
    void validateAnnotatedObject_IllegalNullValue() {
        Message message = new GenericMessage(new MessageType("message"), new JSR303AnnotatedInstance(null));
        ProcessingContext context = null;
        testSubject
                .interceptOnDispatch(message, context, dispatchInterceptorChain)
                .error()
                .ifPresentOrElse(e -> {
                                     assertThat(e).isInstanceOf(JSR303ViolationException.class);
                                     assertThat(((JSR303ViolationException) e).getViolations()).isNotEmpty();
                                 }, () -> fail("Expected exception, but got none.")
                );
        verify(dispatchInterceptorChain, never()).proceed(message, context);
    }

    @Test
    void validateAnnotatedObject_LegalValue() {
        Message message = new GenericMessage(
                new MessageType("message"), new JSR303AnnotatedInstance("abc")
        );
        ProcessingContext context = null;
        testSubject.interceptOnDispatch(message, context, dispatchInterceptorChain);
        verify(dispatchInterceptorChain).proceed(message, context);
    }

    @Test
    void validateAnnotatedObject_IllegalValue() {
        Message message = new GenericMessage(
                new MessageType("message"), new JSR303AnnotatedInstance("bea")
        );
        testSubject.interceptOnDispatch(message, null, dispatchInterceptorChain)
                   .error()
                   .ifPresentOrElse(e -> {
                                        assertThat(e).isInstanceOf(JSR303ViolationException.class);
                                        assertThat(((JSR303ViolationException) e).getViolations()).isNotEmpty();
                                    }, () -> fail("Expected exception, but got none.")
                   );
        verify(dispatchInterceptorChain, never()).proceed(message, null);
    }

    @Test
    void customValidatorFactory() {
        Message message = new GenericMessage(new MessageType("message"),
                                             new JSR303AnnotatedInstance("abc"));
        ValidatorFactory mockValidatorFactory = spy(Validation.buildDefaultValidatorFactory());
        testSubject = new BeanValidationInterceptor<>(mockValidatorFactory);
        testSubject.interceptOnDispatch(message, null, dispatchInterceptorChain);
        verify(mockValidatorFactory).getValidator();
    }

    public static class JSR303AnnotatedInstance {

        @SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal", "unused"})
        @Pattern(regexp = "ab.*")
        @NotNull
        private String notNull;

        public JSR303AnnotatedInstance(String notNull) {
            this.notNull = notNull;
        }
    }
}
