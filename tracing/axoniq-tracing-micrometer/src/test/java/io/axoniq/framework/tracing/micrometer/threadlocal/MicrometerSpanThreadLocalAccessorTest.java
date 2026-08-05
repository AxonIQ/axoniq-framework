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

package io.axoniq.framework.tracing.micrometer.threadlocal;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerSpanThreadLocalAccessorTest {

    private SdkTracerProvider tracerProvider;
    private Tracer tracer;
    private MicrometerSpanThreadLocalAccessor testSubject;

    @BeforeEach
    void setUp() {
        tracerProvider = SdkTracerProvider.builder().build();
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                                                         .setTracerProvider(tracerProvider)
                                                         .build();
        tracer = new OtelTracer(openTelemetry.getTracer("AxoniqFramework"),
                                new OtelCurrentTraceContext(),
                                event -> {
                                });
        testSubject = new MicrometerSpanThreadLocalAccessor(ObservationRegistry.create(), tracer);
    }

    @AfterEach
    void tearDown() {
        tracerProvider.close();
    }

    @Test
    void restoresEveryNestedSpanAndClearScopeInLifoOrder() {
        Span parent = tracer.nextSpan().name("parent").start();

        testSubject.setValue(parent);
        assertThat(tracer.currentSpan()).isEqualTo(parent);

        testSubject.setValue(parent);
        assertThat(tracer.currentSpan()).isEqualTo(parent);

        testSubject.setValue();
        assertThat(tracer.currentSpan()).isNull();

        testSubject.setValue(parent);
        assertThat(tracer.currentSpan()).isEqualTo(parent);

        testSubject.restore();
        assertThat(tracer.currentSpan()).isNull();

        testSubject.restore(parent);
        assertThat(tracer.currentSpan()).isEqualTo(parent);

        testSubject.restore(parent);
        assertThat(tracer.currentSpan()).isEqualTo(parent);

        testSubject.restore();
        assertThat(tracer.currentSpan()).isNull();
    }

    @Test
    void clearWithoutAnOwnedRawSpanScopeDoesNotHideTheCurrentHostSpan() {
        Span hostSpan = tracer.nextSpan().name("host-observation").start();

        try (Tracer.SpanInScope ignored = tracer.withSpan(hostSpan)) {
            testSubject.setValue();
            assertThat(tracer.currentSpan()).isEqualTo(hostSpan);

            testSubject.restore();
            assertThat(tracer.currentSpan()).isEqualTo(hostSpan);
        } finally {
            hostSpan.end();
        }

        assertThat(tracer.currentSpan()).isNull();
    }
}
