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

package io.axoniq.framework.tracing.micrometer;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.test.simple.SimpleTracer;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.tracing.SpanScope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link ProcessingContextAccessor}: it reads the active Micrometer span from a {@link ProcessingContext} (via
 * {@link MicrometerSpanFactory#rawSpanFrom}, keyed by the span thread-local key). {@code writeValues} is a required
 * part of the {@link io.micrometer.context.ContextAccessor} contract but is a deliberate no-op — see the class
 * Javadoc for why.
 */
class ProcessingContextAccessorTest {

    private final ProcessingContextAccessor accessor = new ProcessingContextAccessor();
    private Span span;

    @BeforeEach
    void setUp() {
        span = new SimpleTracer().spanBuilder().name("segment").start();
    }

    @Test
    void readableAndWriteableTypeAreProcessingContext() {
        assertThat(accessor.readableType()).isEqualTo(ProcessingContext.class);
        assertThat(accessor.writeableType()).isEqualTo(ProcessingContext.class);
    }

    @Test
    void readValueReturnsTheActiveSpanUnderTheSpanKey() {
        // given a context carrying the span on a branch (the single-key, branch-carried model)
        ProcessingContext source = aContextCarrying(span);

        // when / then
        assertThat((Span) accessor.readValue(source, ProcessingContextAccessor.SPAN_KEY)).isSameAs(span);
        assertThat((Object) accessor.readValue(source, "unrelated.key")).isNull();
    }

    @Test
    void readValuesPutsTheActiveSpanIntoTheTargetWhenKeyMatches() {
        // given
        ProcessingContext source = aContextCarrying(span);
        Map<Object, Object> target = new HashMap<>();

        // when
        accessor.readValues(source, key -> true, target);

        // then
        assertThat(target).containsEntry(ProcessingContextAccessor.SPAN_KEY, span);
    }

    @Test
    void readValuesSkipsWhenNoActiveSpanIsPresent() {
        // given a context without an active span
        ProcessingContext source = new StubProcessingContext();
        Map<Object, Object> target = new HashMap<>();

        // when
        accessor.readValues(source, key -> true, target);

        // then
        assertThat(target).isEmpty();
    }

    @Test
    void writeValuesIsANoOpPassthrough() {
        // given a captured value map and a context that does not yet carry an active span
        Map<Object, Object> captured = Map.of(ProcessingContextAccessor.SPAN_KEY, span);
        ProcessingContext target = new StubProcessingContext();

        // when
        ProcessingContext result = accessor.writeValues(captured, target);

        // then the context is returned unchanged -- no active span was written
        assertThat(result).isSameAs(target);
        assertThat(MicrometerSpanFactory.rawSpanFrom(result)).isNull();
    }

    /**
     * Carries {@code rawSpan} as the active span on a branch of a fresh context, exactly as a real
     * {@link MicrometerSpan} does via {@link SpanScope#addToContext}, so {@code readValue}/{@code readValues} tests
     * exercise the same single-key, branch-carried representation this accessor reads from in production. A minimal
     * test-local {@link RawSpanCarrier} stands in for {@link MicrometerSpan} since the test needs to carry a specific,
     * already-built raw span rather than one materialized fresh by a factory.
     */
    private ProcessingContext aContextCarrying(Span rawSpan) {
        org.axonframework.messaging.tracing.Span axonSpan = new RawSpanCarryingSpan(rawSpan);
        return SpanScope.addToContext(new StubProcessingContext(), new RawSpanCarryingScope(axonSpan));
    }

    private record RawSpanCarryingScope(org.axonframework.messaging.tracing.Span span) implements SpanScope {

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
            // No-op: nothing in these tests starts or owns this span's lifecycle.
        }

        @Override
        public <T> T within(Supplier<T> operation) {
            return operation.get();
        }
    }

    private record RawSpanCarryingSpan(Span rawSpan) implements org.axonframework.messaging.tracing.Span,
                                                                 RawSpanCarrier {

        @Override
        public SpanScope start() {
            throw new UnsupportedOperationException("Test double: already carried on a scope, never started again.");
        }

        @Override
        public org.axonframework.messaging.tracing.Span addAttribute(String key, String value) {
            return this;
        }

        @Override
        public org.axonframework.messaging.tracing.Span recordException(Throwable t) {
            return this;
        }

        @Override
        public <M extends Message> M propagateContext(M message) {
            return message;
        }
    }
}
