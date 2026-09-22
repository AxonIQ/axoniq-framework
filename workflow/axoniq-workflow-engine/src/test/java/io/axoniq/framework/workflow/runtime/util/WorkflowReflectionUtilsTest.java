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
package io.axoniq.framework.workflow.runtime.util;

import org.junit.jupiter.api.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

class WorkflowReflectionUtilsTest {

    @Test
    void testConstructorIsPrivate() throws NoSuchMethodException {
        Constructor<WorkflowReflectionUtils> constructor = WorkflowReflectionUtils.class.getDeclaredConstructor();
        assertThat(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers())).isTrue();
        constructor.setAccessible(true);
        assertThatCode(constructor::newInstance).doesNotThrowAnyException();
    }

    @Test
    void testCreateDefaultInstance() {
        Optional<String> instance = WorkflowReflectionUtils.createDefaultInstance(String.class);
        assertThat(instance).contains("");
    }

    @Test
    void testCreateDefaultInstanceFailure() {
        // Class without default constructor
        class NoDefaultConstructor {
            NoDefaultConstructor(String s) {}
        }
        Optional<NoDefaultConstructor> instance = WorkflowReflectionUtils.createDefaultInstance(NoDefaultConstructor.class);
        assertThat(instance).isEmpty();
    }

    @Test
    void testRequireIsAssignableFrom() {
        assertThat(WorkflowReflectionUtils.requireIsAssignableFrom(CharSequence.class, String.class)).isEqualTo(String.class);
    }

    @Test
    void testRequireIsAssignableFromFailure() {
        assertThatThrownBy(() ->
            WorkflowReflectionUtils.requireIsAssignableFrom(String.class, Integer.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInvoke() throws NoSuchMethodException {
        Method method = String.class.getMethod("length");
        Object result = WorkflowReflectionUtils.invoke("hello", method);
        assertThat(result).isEqualTo(5);
    }

    @Test
    void testInvokeWithArgs() throws NoSuchMethodException {
        Method method = String.class.getMethod("substring", int.class, int.class);
        Object result = WorkflowReflectionUtils.invoke("hello", method, 1, 3);
        assertThat(result).isEqualTo("el");
    }

    @Test
    void testInvokeInvocationTargetException() throws NoSuchMethodException {
        Method method = TestTarget.class.getMethod("throwException");
        TestTarget target = new TestTarget();
        assertThatThrownBy(() ->
            WorkflowReflectionUtils.invoke(target, method))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .extracting(Throwable::getCause)
                .satisfies(cause -> assertThat(cause.getMessage()).isEqualTo("test exception"));
    }

    @Test
    void testInvokeIllegalAccessException() throws NoSuchMethodException {
        Method method = TestTarget.class.getDeclaredMethod("privateMethod");
        TestTarget target = new TestTarget();
        assertThatThrownBy(() ->
            WorkflowReflectionUtils.invoke(target, method))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot access");
    }

    public static class TestTarget {
        public void throwException() {
            throw new RuntimeException("test exception");
        }
        private void privateMethod() {}
    }
}
