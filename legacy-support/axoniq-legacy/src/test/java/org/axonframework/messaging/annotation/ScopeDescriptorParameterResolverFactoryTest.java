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

package org.axonframework.messaging.annotation;

import org.axonframework.messaging.NoScopeDescriptor;
import org.axonframework.messaging.Scope;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link ScopeDescriptorParameterResolverFactory}, in particular that it recognizes any
 * handler method declaring a {@link ScopeDescriptor}-typed parameter (no annotation restriction, unlike
 * {@code SagaLifecycleParameterResolverFactory}), and that the resulting {@link ParameterResolver} resolves the
 * description of the current {@link Scope}, falling back to {@link NoScopeDescriptor#INSTANCE} when none is active.
 */
class ScopeDescriptorParameterResolverFactoryTest {

    private final ScopeDescriptorParameterResolverFactory testSubject = new ScopeDescriptorParameterResolverFactory();

    @Nested
    class CreateInstance {

        @Test
        void returnsResolverForAnyMethodWithScopeDescriptorParameter() throws NoSuchMethodException {
            // when
            var resolver = createInstanceFor(SomeHandler.class, "handleWithoutAnnotation", Object.class,
                                             ScopeDescriptor.class);

            // then
            assertThat(resolver).isNotNull();
        }

        @Test
        void returnsResolverForAnnotatedMethodWithScopeDescriptorParameterToo() throws NoSuchMethodException {
            // when
            var resolver = createInstanceFor(SomeHandler.class, "handleWithSomeAnnotation", Object.class,
                                             ScopeDescriptor.class);

            // then
            assertThat(resolver).isNotNull();
        }

        @Test
        void returnsNullWhenParameterIsNotOfTypeScopeDescriptor() throws NoSuchMethodException {
            // when
            var resolver = createInstanceFor(SomeHandler.class, "handleWithoutScopeDescriptor", Object.class);

            // then
            assertThat(resolver).isNull();
        }

        private ParameterResolver<?> createInstanceFor(Class<?> declaringClass,
                                                       String methodName,
                                                       Class<?>... parameterTypes) throws NoSuchMethodException {
            Method method = declaringClass.getDeclaredMethod(methodName, parameterTypes);
            Parameter[] parameters = method.getParameters();
            return testSubject.createInstance(method, parameters, parameters.length - 1);
        }
    }

    @Nested
    class Resolving {

        private final ParameterResolver<ScopeDescriptor> resolver = createScopeDescriptorParameterResolver();

        @Test
        void matchesAlwaysReturnsTrue() {
            // when / then
            assertThat(resolver.matches(new StubProcessingContext())).isTrue();
        }

        @Test
        void resolveParameterValueReturnsTheDescriptorOfTheCurrentScope() {
            // given
            TestScope scope = new TestScope();

            // when
            ScopeDescriptor resolved = scope.run(
                    () -> resolver.resolveParameterValue(new StubProcessingContext())
                                  .orTimeout(50, TimeUnit.MILLISECONDS)
                                  .join()
            );

            // then
            assertThat(resolved).isSameAs(scope.descriptor);
        }

        @Test
        void resolveParameterValueFallsBackToNoScopeDescriptorWhenNoScopeIsActive() {
            // when
            ScopeDescriptor resolved = resolver.resolveParameterValue(new StubProcessingContext())
                                               .orTimeout(50, TimeUnit.MILLISECONDS)
                                               .join();

            // then
            assertThat(resolved).isSameAs(NoScopeDescriptor.INSTANCE);
        }

        private ParameterResolver<ScopeDescriptor> createScopeDescriptorParameterResolver() {
            try {
                Method method = SomeHandler.class.getDeclaredMethod("handleWithoutAnnotation", Object.class,
                                                                    ScopeDescriptor.class);
                Parameter[] parameters = method.getParameters();
                return testSubject.createInstance(method, parameters, 1);
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException(e);
            }
        }

        private static final class TestScope extends Scope {

            private final ScopeDescriptor descriptor = () -> "TestScope";

            private <R> R run(Supplier<R> task) {
                startScope();
                try {
                    return task.get();
                } finally {
                    endScope();
                }
            }

            @Override
            public ScopeDescriptor describeScope() {
                return descriptor;
            }
        }
    }

    @SuppressWarnings("unused")
    private static class SomeHandler {

        public void handleWithoutAnnotation(Object event, ScopeDescriptor scope) {
        }

        @Deprecated
        public void handleWithSomeAnnotation(Object event, ScopeDescriptor scope) {
        }

        public void handleWithoutScopeDescriptor(Object event) {
        }
    }
}
