package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.util.WorkflowReflectionUtils;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class WorkflowReflectionUtilsTest {

    @Test
    void testConstructorIsPrivate() throws NoSuchMethodException {
        Constructor<WorkflowReflectionUtils> constructor = WorkflowReflectionUtils.class.getDeclaredConstructor();
        assertTrue(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        assertDoesNotThrow(() -> { constructor.newInstance(); });
    }

    @Test
    void testCreateDefaultInstance() {
        Optional<String> instance = WorkflowReflectionUtils.createDefaultInstance(String.class);
        assertTrue(instance.isPresent());
        assertEquals("", instance.get());
    }

    @Test
    void testCreateDefaultInstanceFailure() {
        // Class without default constructor
        class NoDefaultConstructor {
            NoDefaultConstructor(String s) {}
        }
        Optional<NoDefaultConstructor> instance = WorkflowReflectionUtils.createDefaultInstance(NoDefaultConstructor.class);
        assertFalse(instance.isPresent());
    }

    @Test
    void testRequireIsAssignableFrom() {
        assertEquals(String.class, WorkflowReflectionUtils.requireIsAssignableFrom(CharSequence.class, String.class));
    }

    @Test
    void testRequireIsAssignableFromFailure() {
        assertThrows(IllegalArgumentException.class, () ->
            WorkflowReflectionUtils.requireIsAssignableFrom(String.class, Integer.class));
    }

    @Test
    void testInvoke() throws NoSuchMethodException {
        Method method = String.class.getMethod("length");
        Object result = WorkflowReflectionUtils.invoke("hello", method);
        assertEquals(5, result);
    }

    @Test
    void testInvokeWithArgs() throws NoSuchMethodException {
        Method method = String.class.getMethod("substring", int.class, int.class);
        Object result = WorkflowReflectionUtils.invoke("hello", method, 1, 3);
        assertEquals("el", result);
    }

    @Test
    void testInvokeInvocationTargetException() throws NoSuchMethodException {
        Method method = TestTarget.class.getMethod("throwException");
        TestTarget target = new TestTarget();
        RuntimeException ex = assertThrows(RuntimeException.class, () ->
            WorkflowReflectionUtils.invoke(target, method));
        assertEquals("test exception", ex.getCause().getMessage());
    }

    @Test
    void testInvokeIllegalAccessException() throws NoSuchMethodException {
        Method method = TestTarget.class.getDeclaredMethod("privateMethod");
        TestTarget target = new TestTarget();
        RuntimeException ex = assertThrows(RuntimeException.class, () ->
            WorkflowReflectionUtils.invoke(target, method));
        assertTrue(ex.getMessage().contains("Cannot access"));
    }

    public static class TestTarget {
        public void throwException() {
            throw new RuntimeException("test exception");
        }
        private void privateMethod() {}
    }
}
