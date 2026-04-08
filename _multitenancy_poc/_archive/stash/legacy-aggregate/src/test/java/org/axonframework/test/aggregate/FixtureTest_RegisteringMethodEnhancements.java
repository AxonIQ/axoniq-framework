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

package org.axonframework.test.aggregate;

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.annotation.HandlerDefinition;
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.jspecify.annotations.NonNull;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.axonframework.test.matchers.Matchers.exactSequenceOf;
import static org.axonframework.test.matchers.Matchers.predicate;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test class is intended to test whether the registration of a {@link ParameterResolverFactory}, a
 * {@link HandlerDefinition} and a {@link HandlerEnhancerDefinition} go according to plan.
 *
 * @author Steven van Beelen
 */
public class FixtureTest_RegisteringMethodEnhancements {

    private static final String TEST_AGGREGATE_IDENTIFIER = "aggregate-identifier";

    private FixtureConfiguration<AnnotatedAggregate> testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new AggregateTestFixture<>(AnnotatedAggregate.class);
        testSubject.registerInjectableResource(new HardToCreateResource());
    }

    @Test
    @Disabled("TODO #3195 - Migration Module")
    void registerParameterResolverFactory() {
        testSubject.registerParameterResolverFactory(new TestParameterResolverFactory(new AtomicBoolean(false)))
                   .given(new MyEvent(TEST_AGGREGATE_IDENTIFIER, 42))
                   .when(new ResolveParameterCommand(TEST_AGGREGATE_IDENTIFIER))
                   .expectEventsMatching(exactSequenceOf(predicate(eventMessage -> {
                       Object payload = eventMessage.payload();
                       assertTrue(payload instanceof ParameterResolvedEvent);
                       AtomicBoolean assertion = ((ParameterResolvedEvent) payload).getAssertion();
                       return assertion.get();
                   })));
    }

    @Test
    @Disabled("TODO #3195 - Migration Module")
    void createHandlerMethodIsCalledForRegisteredCustomHandlerDefinition() {
        AtomicBoolean handlerDefinitionReached = new AtomicBoolean(false);

        testSubject.registerHandlerDefinition(new TestHandlerDefinition(handlerDefinitionReached))
                   .givenNoPriorActivity()
                   .when(new CreateAggregateCommand(TEST_AGGREGATE_IDENTIFIER))
                   .expectEventsMatching(exactSequenceOf(predicate(
                           eventMessage -> eventMessage.payloadType().isAssignableFrom(MyEvent.class)
                   )));

        assertTrue(handlerDefinitionReached.get());
    }

    @Test
    @Disabled("TODO #3195 - Migration Module")
    void wrapHandlerMethodIsCalledForRegisteredCustomHandlerEnhancerDefinition() {
        AtomicBoolean handlerEnhancerReached = new AtomicBoolean(false);

        testSubject.registerHandlerEnhancerDefinition(new TestHandlerEnhancerDefinition(handlerEnhancerReached))
                   .givenNoPriorActivity()
                   .when(new CreateAggregateCommand(TEST_AGGREGATE_IDENTIFIER))
                   .expectEventsMatching(exactSequenceOf(predicate(
                           eventMessage -> eventMessage.payloadType().isAssignableFrom(MyEvent.class)
                   )));

        assertTrue(handlerEnhancerReached.get());
    }

    private static class TestParameterResolverFactory
            implements ParameterResolverFactory, ParameterResolver<AtomicBoolean> {

        private final AtomicBoolean assertion;

        private TestParameterResolverFactory(AtomicBoolean assertion) {
            this.assertion = assertion;
        }

        @Nullable
        @Override
        public ParameterResolver<AtomicBoolean> createInstance(Executable executable,
                                                               Parameter[] parameters,
                                                               int parameterIndex) {
            return AtomicBoolean.class.equals(parameters[parameterIndex].getType()) ? this : null;
        }

        @NonNull
        @Override
        public CompletableFuture<AtomicBoolean> resolveParameterValue(ProcessingContext context) {
            return CompletableFuture.completedFuture(assertion);
        }

        @Override
        public boolean matches(ProcessingContext context) {
            Message message = Message.fromContext(context);
            return message.payloadType().isAssignableFrom(ResolveParameterCommand.class);
        }
    }

    private static class TestHandlerDefinition implements HandlerDefinition {

        private final AtomicBoolean assertion;

        public TestHandlerDefinition(AtomicBoolean assertion) {
            this.assertion = assertion;
        }

        @Override
        public <T> Optional<MessageHandlingMember<T>> createHandler(
                Class<T> declaringType,
                Method method,
                ParameterResolverFactory parameterResolverFactory,
                Function<Object, MessageStream<?>> messageStreamResolver
        ) {
            assertion.set(true);
            // We do not care about a specific MessageHandlingMember,
            //  only that this method is called to ensure its part of the FixtureConfiguration.
            return Optional.empty();
        }
    }

    private static class TestHandlerEnhancerDefinition implements HandlerEnhancerDefinition {

        private final AtomicBoolean assertion;

        private TestHandlerEnhancerDefinition(AtomicBoolean assertion) {
            this.assertion = assertion;
        }

        @Override
        public @NonNull
        <T> MessageHandlingMember<T> wrapHandler(MessageHandlingMember<T> original) {
            assertion.set(true);
            // We do not care about a specific MessageHandlingMember,
            //  only that this method is called to ensure its part of the FixtureConfiguration.
            return original;
        }
    }
}
