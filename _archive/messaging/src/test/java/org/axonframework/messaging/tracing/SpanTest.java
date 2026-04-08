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

package org.axonframework.messaging.tracing;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class SpanTest {

    private TestSpan span = new TestSpan();

    @Test
    void runRunnableWorks() {
        span.run(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
        });
        assertTrue(span.ended);
    }

    @Test
    void runRunnableRegistersException() {
        RuntimeException exception = new RuntimeException("My custom exception");
        assertThrows(RuntimeException.class, () -> span.run(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
            throw exception;
        }));
        assertTrue(span.ended);
        assertEquals(exception, span.exception);
    }

    @Test
    void wrapRunnableWorks() {
        span.wrapRunnable(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
        }).run();
        assertTrue(span.ended);
    }

    @Test
    void wrapRunnableRegistersException() {
        RuntimeException exception = new RuntimeException("My custom exception");
        Runnable wrappedRunnable = span.wrapRunnable(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
            throw exception;
        });
        assertThrows(RuntimeException.class, wrappedRunnable::run);
        assertTrue(span.ended);
        assertEquals(exception, span.exception);
    }

    @Test
    void runSupplierWorks() {
        String result = span.runSupplier(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
            return "SupplyString";
        });
        assertTrue(span.ended);
        assertEquals("SupplyString", result);
    }

    @Test
    void runSupplierRegistersException() {
        RuntimeException exception = new RuntimeException("My custom exception");
        assertThrows(RuntimeException.class, () -> span.runSupplier(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
            throw exception;
        }));
        assertTrue(span.ended);
        assertEquals(exception, span.exception);
    }

    @Test
    void wrapSupplierWorks() {
        String result = span.wrapSupplier(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
            return "SupplyString";
        }).get();
        assertTrue(span.ended);
        assertEquals("SupplyString", result);
    }

    @Test
    void wrapSupplierRegistersException() {
        RuntimeException exception = new RuntimeException("My custom exception");
        Supplier<Object> wrappedSupplier = span.wrapSupplier(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
            throw exception;
        });
        assertThrows(RuntimeException.class, wrappedSupplier::get);
        assertTrue(span.ended);
        assertEquals(exception, span.exception);
    }

    @Test
    void runCallableWorks() throws Exception {
        String result = span.runCallable(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
            return "CallString";
        });
        assertTrue(span.ended);
        assertEquals("CallString", result);
    }

    @Test
    void runCallableRegistersRuntimeException() {
        RuntimeException exception = new RuntimeException("My custom exception");
        assertThrows(RuntimeException.class, () -> span.runCallable(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
            throw exception;
        }));
        assertTrue(span.ended);
        assertEquals(exception, span.exception);
    }

    @Test
    void runCallableRegistersCheckedException() {
        IOException exception = new IOException("My custom exception");
        assertThrows(IOException.class, () -> span.runCallable(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
            throw exception;
        }));
        assertTrue(span.ended);
        assertEquals(exception, span.exception);
    }

    @Test
    void wrapCallableWorks() throws Exception {
        String result = span.wrapCallable(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
            return "CallString";
        }).call();
        assertTrue(span.ended);
        assertEquals("CallString", result);
    }

    @Test
    void wrapCallableRegistersRuntimeException() {
        RuntimeException exception = new RuntimeException("My custom exception");
        Callable<Object> wrappedCallable = span.wrapCallable(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
            throw exception;
        });
        assertThrows(RuntimeException.class, wrappedCallable::call);
        assertTrue(span.ended);
        assertEquals(exception, span.exception);
    }

    @Test
    void wrapCallableRegistersCheckedException() {
        IOException exception = new IOException("My custom exception");
        Callable<Object> wrappedCallable = span.wrapCallable(() -> {
            assertTrue(span.started);
            assertFalse(span.ended);
            throw exception;
        });
        assertThrows(IOException.class, wrappedCallable::call);
        assertTrue(span.ended);
        assertEquals(exception, span.exception);
    }

    static class TestSpan implements Span {

        boolean started;
        boolean ended;
        Throwable exception;

        @Override
        public @NonNull Span start() {
            this.started = true;
            return this;
        }

        @Override
        public @NonNull SpanScope makeCurrent() {
            return () -> {};
        }

        @Override
        public void end() {
            this.ended = true;
        }

        @Override
        public @NonNull Span recordException(@NonNull Throwable t) {
            this.exception = t;
            return this;
        }

        @Override
        public @NonNull Span addAttribute(@NonNull String key, @Nullable String value) {
            return this;
        }
    }
}
